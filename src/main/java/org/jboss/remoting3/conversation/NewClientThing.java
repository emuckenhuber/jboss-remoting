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
import org.jboss.remoting3.HandleableCloseable;
import org.jboss.remoting3.MessageInputStream;
import org.jboss.remoting3.MessageOutputStream;
import org.xnio.FutureResult;
import org.xnio.IoFuture;
import org.xnio.IoUtils;
import org.xnio.OptionMap;

/**
 * @author Emanuel Muckenhuber
 */
public class NewClientThing extends ConversationProtocolHandler<ConversationChannelClient> implements ConversationChannelClient, Channel.Receiver {

    private final Channel channel;
    private final ConversationMessageReceiver messageReceiver;
    private final AtomicInteger conversationIDs = new AtomicInteger();
    private final ConcurrentMap<Integer, ConversationProtocolHandler> conversations = new ConcurrentHashMap<Integer, ConversationProtocolHandler>(8, 0.75f, Runtime.getRuntime().availableProcessors());

    public NewClientThing(final Channel channel, final ConversationMessageReceiver messageReceiver, final Executor executor) {
        super(executor);
        this.channel = channel;
        this.messageReceiver = messageReceiver;
    }

    @Override
    public void handleError(Channel channel, IOException error) {
        channel.closeAsync();
    }

    @Override
    public void handleEnd(Channel channel) {
        closeAsync();
    }

    @Override
    public void handleMessage(Channel channel, MessageInputStream message) {
        try {
            final int conversationID = readInt(message);
            final int messageType = message.read();
            handle(conversationID, messageType, channel, message);
            channel.receiveMessage(this);
        } catch (IOException e) {
            handleError(channel, e);
        }
    }

    @Override
    public IoFuture<Conversation> openConversation(OptionMap options) throws IOException {
        final FutureResult<Conversation> result = new FutureResult<Conversation>();
        final IoFuture<Conversation> future = result.getIoFuture();
        future.addNotifier(new IoFuture.Notifier<Conversation, Void>() {
            @Override
            public void notify(IoFuture<? extends Conversation> future, Void attachment) {
                resourceFinished();
            }
        }, null);

        for (;;) {
            final int conversationID = conversationIDs.incrementAndGet();
            final PendingConversation pending = new PendingConversation(conversationID, result, getExecutor());
            if (conversations.putIfAbsent(conversationID, pending) == null) {
                final MessageOutputStream os = channel.writeMessage();
                try {
                    writeInt(os, conversationID);
                    os.write(OPEN_CONVERSATION);
                    os.write(Protocol.CAP_VERSION);
                    os.write(Protocol.VERSION);
                    os.close();
                } catch (IOException e) {
                    pending.handleError(null, e);
                    throw e;
                } finally {
                    IoUtils.safeClose(os);
                }
                return future;
            }
        }
    }

    @Override
    ConversationProtocolHandler getProtocolHandler(int conversationID) {
        return conversations.get(conversationID);
    }

    class PendingConversation extends ConversationProtocolHandler {

        final int localConversationID;
        final FutureResult<Conversation> result;
        PendingConversation(final int localConversationID, final FutureResult<Conversation> result, Executor executor) {
            super(executor);
            this.result = result;
            this.localConversationID = localConversationID;
        }

        @Override
        ConversationProtocolHandler getProtocolHandler(int conversationID) {
            return this;
        }

        @Override
        void conversationOpenend(MessageInputStream message) throws IOException {
            final FutureResult<Conversation> result = this.result;
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

                    final NewConversationImpl conversation = new NewConversationImpl(conversationID, messageReceiver, channel, getExecutor());
                    if (conversations.replace(localConversationID, this, conversation)) {
                        setResult(conversation);
                        conversation.addCloseHandler(new CloseHandler<Conversation>() {
                            @Override
                            public void handleClose(Conversation closed, IOException exception) {
                                conversationFinished(conversationID, conversation);
                            }
                        });
                        ok = true;
                    } else {
                        result.setException(new IOException("duplicate registration"));
                        sendProtocolError(conversationID, channel, "duplicate registration");
                        // conversation.handleRemoteClose();
                        return;
                    }
                } catch (IOException e) {
                    ProtocolUtils.sendProtocolError(conversationID, channel, "failed to register conversation " + conversationID);
                } finally {
                    if (!ok) {
                        resourceFinished();
                    }
                }
            } catch (IOException e) {
                result.setException(e);
            } finally {
                IoUtils.safeClose(message);
            }
        }

        void setResult(final Conversation conversation) {
            if (prepareClose()) {
                result.setResult(conversation);
            }
        }

        @Override
        void handleError(MessageInputStream message, IOException error) throws IOException {
            IoUtils.safeClose(message);
            if (prepareClose()) {
                result.setException(error);
            }
        }

        @Override
        protected void closeAction() throws IOException {
            if (prepareClose()) {
                result.setCancelled();
            }
        }
    }

    protected void conversationFinished(final int conversationID, final ConversationProtocolHandler conversation) {
        if (conversations.remove(conversationID, conversation)) {
            resourceFinished();
            return;
        } else {
            ConversationLogger.ROOT_LOGGER.debugf("failed to unregister conversation %s", conversationID);
        }
    }

    @Override
    protected void closeAction() throws IOException {
        if (prepareClose()) {
            for (final ConversationProtocolHandler handler : conversations.values()) {
                handler.closeAsync();
            }
        }
    }
}
