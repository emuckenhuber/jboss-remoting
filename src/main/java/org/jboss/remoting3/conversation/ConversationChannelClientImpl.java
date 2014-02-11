/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2014, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.remoting3.conversation;

import static org.jboss.remoting3.conversation.Protocol.CAP_VERSION;
import static org.jboss.remoting3.conversation.Protocol.OPEN_CONVERSATION;
import static org.jboss.remoting3.conversation.Protocol.VERSION;
import static org.jboss.remoting3.conversation.ProtocolUtils.expectHeader;
import static org.jboss.remoting3.conversation.ProtocolUtils.readInt;
import static org.jboss.remoting3.conversation.ProtocolUtils.sendProtocolError;
import static org.jboss.remoting3.conversation.ProtocolUtils.writeInt;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.remoting3.Channel;
import org.jboss.remoting3.CloseHandler;
import org.jboss.remoting3.MessageInputStream;
import org.jboss.remoting3.MessageOutputStream;
import org.xnio.FutureResult;
import org.xnio.IoFuture;
import org.xnio.IoUtils;
import org.xnio.OptionMap;

/**
 * @author Emanuel Muckenhuber
 */
class ConversationChannelClientImpl extends ConversationResourceTracker<ConversationChannelClient> implements Channel.Receiver, ConversationChannelClient {

    private final Channel channel;
    private final Executor executor;
    private final AtomicInteger pendingConnectionIDs = new AtomicInteger();
    private final ConcurrentMap<Integer, ConversationImpl> conversations = new ConcurrentHashMap<Integer, ConversationImpl>(16, 0.75f, Runtime.getRuntime().availableProcessors());
    private final ConcurrentMap<Integer, PendingConversation> pendingConversations = new ConcurrentHashMap<Integer, PendingConversation>(16, 0.75f, Runtime.getRuntime().availableProcessors());

    ConversationChannelClientImpl(Channel channel, Executor executor) {
        super(executor);
        this.channel = channel;
        this.executor = executor;
    }

    @Override
    public void handleError(Channel channel, IOException error) {
        channel.closeAsync();
    }

    @Override
    public void handleEnd(Channel channel) {
        //
    }

    @Override
    public void handleMessage(Channel channel, MessageInputStream message) {
        try {
            final int conversationID = readInt(message);
            final int messageType = message.read();
            switch (messageType) {
                case Protocol.CONVERSATION_OPENED: {
                    final PendingConversation conversation = pendingConversations.get(conversationID);
                    if (conversation != null) {
                        conversationOpened(conversation, message);
                    } else {
                        sendProtocolError(conversationID, channel, "no pending connection");
                    }
                    break;
                } case Protocol.CONVERSATION_REJECTED: {
                    final PendingConversation conversation = pendingConversations.get(conversationID);
                    if (conversation != null) {
                        conversation.result.setException(new IOException("rejected"));
                    }
                    break;
                } case Protocol.CONVERSATION_MESSAGE: {
                    final ConversationImpl conversation = conversations.get(conversationID);
                    if (conversation != null) {
                        conversation.handleMessage(conversationID, channel, message);
                    } else {
                        sendProtocolError(conversationID, channel, "no such conversation");
                    }
                    break;
                } case Protocol.CONVERSATION_CLOSED: {
                    final ConversationImpl conversation = conversations.get(conversationID);
                    if (conversation != null) {
                        conversation.handleRemoteClose();
                    }
                    break;
                } default:
                  throw new IOException();
            }
            channel.receiveMessage(this);
        } catch (IOException e) {
            e.printStackTrace();
            handleError(channel, e);
        }
    }

    public IoFuture<Conversation> openConversation(final ConversationMessageReceiver messageReceiver, OptionMap options) throws IOException {
        incrementResourceCount();

        final FutureResult<Conversation> result = new FutureResult<Conversation>();
        final IoFuture<Conversation> future = result.getIoFuture();
        future.addNotifier(new IoFuture.Notifier<Conversation, Void>() {
            @Override
            public void notify(IoFuture<? extends Conversation> future, Void attachment) {
                resourceFinished();
            }
        }, null);

        final PendingConversation pending = new PendingConversation(messageReceiver, result);
        int pendingConnectionID;
        for (;;) {
            pendingConnectionID = pendingConnectionIDs.incrementAndGet();
            if (pendingConversations.putIfAbsent(pendingConnectionID, pending) == null) {
                break;
            }
        }
        final MessageOutputStream os = channel.writeMessage();
        try {
            writeInt(os, pendingConnectionID);
            os.write(OPEN_CONVERSATION);
            os.write(Protocol.CAP_VERSION);
            os.write(Protocol.VERSION);
            os.close();
        } catch (IOException e) {
            result.setException(e);
            throw e;
        } finally {
            IoUtils.safeClose(os);
        }
        return result.getIoFuture();
    }

    protected void conversationOpened(final PendingConversation pending, MessageInputStream message) {
        final FutureResult<Conversation> result = pending.result;
        try {
            incrementResourceCount();
            final Integer conversationID = readInt(message);
            boolean ok = false;
            try {
                expectHeader(message, CAP_VERSION);
                final int version = message.read();
                if (version > VERSION) {
                    throw new IOException();
                }

                message.close();

                final ConversationImpl conversation = new ConversationImpl(conversationID, pending.receiver, channel, executor);
                channel.addCloseHandler(conversation.getCloseHandler());
                conversation.addCloseHandler(new CloseHandler<Conversation>() {
                    @Override
                    public void handleClose(Conversation closed, IOException exception) {
                        conversationClosed(conversationID, conversation);
                    }
                });
                ok = true; // Everything else will be covered by the close handler
                if (conversations.putIfAbsent(conversationID, conversation) != null) {
                    // Won't actually happen, but anyway
                    result.setException(new IOException("duplicate registration"));
                    sendProtocolError(conversationID, channel, "duplicate registration");
                    conversation.handleRemoteClose();
                    return;
                }
                result.setResult(conversation);
            } catch (IOException e) {
                sendProtocolError(conversationID, channel, "failed to register conversation");
                result.setException(e);
            } finally {
                if (!ok) {
                    resourceFinished();
                }
            }
        } catch (IOException e) {
            result.setException(e);
            handleError(channel, e);
        } finally {
            IoUtils.safeClose(message);
        }
    }

    protected void conversationClosed(final int conversationID, final Conversation conversation) {
        if (conversations.remove(conversationID, conversation)) {
            resourceFinished();
            return;
        } else {
            ConversationLogger.ROOT_LOGGER.debugf("failed to unregister conversation %s", conversationID);
        }
    }

    @Override
    protected void closeAction() throws IOException {
        prepareClose();
        for (final PendingConversation pending : pendingConversations.values()) {
            pending.result.setCancelled();
        }
        for (final Conversation conversation : conversations.values()) {
            conversation.closeAsync();
        }
    }

    static class PendingConversation {

        private final ConversationMessageReceiver receiver;
        private final FutureResult<Conversation> result;

        PendingConversation(ConversationMessageReceiver receiver, FutureResult<Conversation> result) {
            this.receiver = receiver;
            this.result = result;
        }
    }

}
