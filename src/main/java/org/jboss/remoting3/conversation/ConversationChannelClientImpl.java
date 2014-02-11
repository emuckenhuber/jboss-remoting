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

import static org.jboss.remoting3.conversation.Protocol.CAP_SINGLE_CONVERSATION;
import static org.jboss.remoting3.conversation.Protocol.CAP_VERSION;
import static org.jboss.remoting3.conversation.Protocol.OPEN_CONVERSATION;
import static org.jboss.remoting3.conversation.Protocol.USER_CAP;
import static org.jboss.remoting3.conversation.Protocol.VERSION;
import static org.jboss.remoting3.conversation.ProtocolUtils.expectHeader;
import static org.jboss.remoting3.conversation.ProtocolUtils.processUserCapabilities;
import static org.jboss.remoting3.conversation.ProtocolUtils.readInt;
import static org.jboss.remoting3.conversation.ProtocolUtils.sendProtocolError;
import static org.jboss.remoting3.conversation.ProtocolUtils.writeInt;
import static org.jboss.remoting3.conversation.ProtocolUtils.writeUserCapabilities;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;

import org.jboss.remoting3.Channel;
import org.jboss.remoting3.CloseHandler;
import org.jboss.remoting3.MessageInputStream;
import org.jboss.remoting3.MessageOutputStream;
import org.jboss.remoting3._private.IntIndexer;
import org.xnio.FutureResult;
import org.xnio.IoFuture;
import org.xnio.IoUtils;
import org.xnio.OptionMap;

/**
 * @author Emanuel Muckenhuber
 */
class ConversationChannelClientImpl extends ConversationResourceTracker<ConversationChannelClient> implements Channel.Receiver, ConversationChannelClient {

    private static final IntIndexer<ConversationHolder> CONVERSATION_INDEXER = new IntIndexer<ConversationHolder>() {
        @Override
        public int getKey(ConversationHolder argument) {
            return argument.conversationID;
        }
    };

    private final ChannelWrapper channel;
    private final Executor executor;
    private final OptionMap options;
    // TODO IntIndexMap.replace() does not work
    // private final IntIndexMap<ConversationHolder> conversations = new IntIndexHashMap<ConversationHolder>(CONVERSATION_INDEXER);
    private final ConcurrentMap<Integer, ConversationHolder> conversations = new ConcurrentHashMap<Integer, ConversationHolder>(8, 0.75f, Runtime.getRuntime().availableProcessors());

    ConversationChannelClientImpl(Channel channel, OptionMap options, Executor executor) {
        super(executor);
        this.channel = new ChannelWrapper(channel, options);
        this.executor = executor;
        this.options = options;
        channel.addCloseHandler(new CloseHandler<Channel>() {
            @Override
            public void handleClose(Channel closed, IOException exception) {
                handleRemoteClosed();
            }
        });
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
    public void handleMessage(Channel ref, MessageInputStream message) {
        try {
            final int conversationID = readInt(message);
            final int messageType = message.read();
            switch (messageType) {
                case Protocol.CONVERSATION_OPENED: {
                    final ConversationHolder conversation = conversations.get(conversationID);
                    if (conversation != null && conversation.isPending()) {
                        conversationOpened(conversationID, conversation, message);
                    } else {
                        sendProtocolError(conversationID, channel, "no pending connection");
                    }
                    break;
                } case Protocol.CONVERSATION_REJECTED: {
                    final ConversationHolder conversation = conversations.get(conversationID);
                    if (conversation != null && conversation.isPending()) {
                        conversation.result.setException(new IOException("conversation rejected."));
                    }
                    break;
                } case Protocol.CONVERSATION_MESSAGE: {
                    final ConversationHolder conversation = conversations.get(conversationID);
                    if (conversation != null && !conversation.isPending()) {
                        conversation.handler.handleMessage(conversationID, message);
                    } else {
                        sendProtocolError(conversationID, channel, "no such conversation " + conversationID);
                    }
                    break;
                } case Protocol.CONVERSATION_CLOSED: {
                    final ConversationHolder conversation = conversations.get(conversationID);
                    if (conversation != null && !conversation.isPending()) {
                        conversation.handler.handleRemoteClose();
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

    @Override
    public IoFuture<Conversation> openConversation(OptionMap options) throws IOException {
        return openConversation(null, options);
    }

    @Override
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

        int pendingConnectionID;
        for (;;) {
            pendingConnectionID = ThreadLocalRandom.current().nextInt();
            final ConversationHolder pending = new ConversationHolder(pendingConnectionID, messageReceiver, result);
            if (conversations.putIfAbsent(pendingConnectionID, pending) == null) {
                break;
            }
        }
        final Capabilities local = options.get(ConversationOptions.USER_CAPABILITIES, Capabilities.EMPTY);
        final MessageOutputStream os = channel.writeMessage();
        try {
            writeInt(os, pendingConnectionID);
            os.write(OPEN_CONVERSATION);
            os.write(Protocol.CAP_VERSION);
            os.write(Protocol.VERSION);

            // Write user capabilities
            writeUserCapabilities(local, os);

            os.close();
        } catch (IOException e) {
            result.setException(e);
            throw e;
        } finally {
            channel.release();
            IoUtils.safeClose(os);
        }
        return result.getIoFuture();
    }

    @Override
    public boolean prepareShutdown() {
        return prepareClose();
    }

    protected void conversationOpened(final int localConversationID, final ConversationHolder pending, MessageInputStream message) {
        final FutureResult<Conversation> result = pending.result;
        try {
            boolean ok = false;
            final Integer conversationID = readInt(message);
            if (incrementResourceCountUnchecked()) {
                try {
                    expectHeader(message, CAP_VERSION);
                    final int version = message.read();
                    if (version > VERSION) {
                        throw new IOException("unsupported version " + version);
                    }
                    // Handle capabilities
                    final Capabilities.Builder builder = Capabilities.newBuilder();
                    for (;;) {
                        final int type = message.read();
                        if (type == -1) {
                            break;
                        }
                        // User capabilities are sent last.
                        if (type == USER_CAP) {
                            final Capabilities local = options.get(ConversationOptions.USER_CAPABILITIES, Capabilities.EMPTY);
                            processUserCapabilities(builder, local, message);
                            break;
                        }
                        // Internal conversation capabilities
                        final int len = readInt(message);
                        switch (type) {
                            case CAP_SINGLE_CONVERSATION:
                                break;
                            default:
                                message.skip(len); // Discard unknown data
                        }
                    }

                    final Capabilities capabilities = builder.build();
                    final ConversationHandlerImpl conversation = new ConversationHandlerImpl(localConversationID, conversationID, pending.receiver, channel, capabilities, executor);
                    conversation.addCloseHandler(new CloseHandler<Conversation>() {
                        @Override
                        public void handleClose(Conversation closed, IOException exception) {
                            conversationClosed(localConversationID, conversation);
                        }
                    });
                    ok = true; // Everything else will be covered by the close handler
                    final ConversationHolder holder = new ConversationHolder(localConversationID, conversation);
                    if (!conversations.replace(localConversationID, pending, holder)) {
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
            } else {
                sendProtocolError(conversationID, channel, "client closed");
            }
        } catch (IOException e) {
            result.setException(e);
            handleError(channel, e);
        } finally {
            IoUtils.safeClose(message);
        }
    }

    protected void conversationClosed(final int conversationID, final Conversation conversation) {
        if (conversations.remove(conversationID) != null) {
            resourceFinished();
            return;
        } else {
            ConversationLogger.ROOT_LOGGER.debugf("failed to unregister conversation %s", conversationID);
        }
    }

    @Override
    protected void closeAction() throws IOException {
        boolean closeAsync = prepareClose();
//        final Iterator<ConversationHolder> i = conversations.iterator();
//        while (i.hasNext()) {
//            final ConversationHolder holder = i.next();
        for (ConversationHolder holder : conversations.values()) {
            if (holder.isPending()) {
                holder.result.setCancelled();
            } else {
                if (closeAsync) {
                    holder.handler.closeAsync();
                } else {
                    holder.handler.handleRemoteClose();
                }
            }
        }
    }

    private void handleRemoteClosed() {
        prepareClose();
        closeAsync();
    }

    static class ConversationHolder {

        private final int conversationID;
        private final ConversationMessageReceiver receiver;
        private final FutureResult<Conversation> result;
        private final ConversationHandlerImpl handler;

        ConversationHolder(int conversationID, ConversationMessageReceiver receiver, FutureResult<Conversation> result) {
            this.conversationID = conversationID;
            this.receiver = receiver;
            this.result = result;
            this.handler = null;
        }

        ConversationHolder(int conversationID, ConversationHandlerImpl handler) {
            this.conversationID = conversationID;
            this.receiver = null;
            this.result = null;
            this.handler = handler;
        }

        boolean isPending() {
            return handler == null;
        }

    }

}
