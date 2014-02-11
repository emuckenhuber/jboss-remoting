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
import static org.jboss.remoting3.conversation.Protocol.CONVERSATION_CLOSED;
import static org.jboss.remoting3.conversation.Protocol.CONVERSATION_MESSAGE;
import static org.jboss.remoting3.conversation.Protocol.CONVERSATION_OPENED;
import static org.jboss.remoting3.conversation.Protocol.CONVERSATION_REJECTED;
import static org.jboss.remoting3.conversation.Protocol.OPEN_CONVERSATION;
import static org.jboss.remoting3.conversation.Protocol.PROTOCOL_ERROR;
import static org.jboss.remoting3.conversation.Protocol.USER_CAP;
import static org.jboss.remoting3.conversation.Protocol.VERSION;
import static org.jboss.remoting3.conversation.ProtocolUtils.processUserCapabilities;
import static org.jboss.remoting3.conversation.ProtocolUtils.readInt;
import static org.jboss.remoting3.conversation.ProtocolUtils.readProtocolError;
import static org.jboss.remoting3.conversation.ProtocolUtils.sendProtocolError;
import static org.jboss.remoting3.conversation.ProtocolUtils.writeInt;
import static org.jboss.remoting3.conversation.ProtocolUtils.writeUserCapabilities;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;

import org.jboss.remoting3.Channel;
import org.jboss.remoting3.CloseHandler;
import org.jboss.remoting3.MessageInputStream;
import org.jboss.remoting3.MessageOutputStream;
import org.jboss.remoting3._private.IntIndexHashMap;
import org.xnio.IoUtils;
import org.xnio.OptionMap;

/**
 * @author Emanuel Muckenhuber
 */
class ConversationsServerHandler extends ConversationResourceTracker<ConversationChannelHandler> implements Channel.Receiver, ConversationChannelHandler {

    private final Executor executor;
    private final OptionMap options;
    private final ConversationMessageReceiver messageListener;

    private final IntIndexHashMap<ConversationHandlerImpl> conversations = new IntIndexHashMap<ConversationHandlerImpl>(ConversationHandlerImpl.CONVERSATION_INT_INDEXER);

    ConversationsServerHandler(ConversationMessageReceiver messageListener, OptionMap options, Executor executor) {
        super(executor);
        this.executor = executor;
        this.options = options;
        this.messageListener = messageListener;
    }

    @Override
    public Channel.Receiver getReceiver() {
        return this;
    }

    @Override
    public void handleError(final Channel channel, final IOException error) {
        channel.closeAsync();
    }

    @Override
    public void handleEnd(final Channel channel) {
        //
    }

    @Override
    public void handleMessage(final Channel channel, final MessageInputStream message) {
        try {
            final int conversationID = readInt(message);
            final int messageType = message.read();
            switch (messageType) {
                case OPEN_CONVERSATION:
                    openConversation(conversationID, channel, message);
                    break;
                case CONVERSATION_MESSAGE: {
                    final ConversationHandlerImpl conversation = conversations.get(conversationID);
                    if (conversation != null) {
                        conversation.handleMessage(conversationID, message);
                    } else {
                        sendProtocolError(conversationID, getWrapper(channel, options), "no such conversation");
                    }
                    break;
                } case CONVERSATION_CLOSED: {
                    final ConversationHandlerImpl conversation = conversations.get(conversationID);
                    if (conversation != null) {
                        conversation.handleRemoteClose();
                    } else {
                        sendProtocolError(conversationID, getWrapper(channel, options), "no such conversation");
                    }
                    break;
                } case PROTOCOL_ERROR: {
                    try {
                        final String error = readProtocolError(message);
                        final ConversationHandlerImpl conversation = conversations.get(conversationID);
                        if (conversation != null) {
                            conversation.handleError(new IOException(error));
                        }
                    } catch (IOException e) {
                        ConversationLogger.ROOT_LOGGER.debugf(e, "failed to process protocol error");
                    } finally {
                        IoUtils.safeClose(message);
                    }
                    break;
                } default: {
                    throw new IOException();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            handleError(channel, e);
        }
        channel.receiveMessage(this);
    }

    @Override
    public boolean prepareShutdown() {
        return super.prepareClose();
    }

    protected void openConversation(final int clientConversationID, final Channel underlyingChannel, final MessageInputStream message) {
        // Get the channel wrapper
        final ChannelWrapper channel = getWrapper(underlyingChannel, options);
        try {
            if (incrementResourceCountUnchecked()) {
                try {
                    boolean singleConversationPerChannel = false;

                    // Conversation negotiation
                    ProtocolUtils.expectHeader(message, CAP_VERSION);
                    final int version = Math.min(VERSION, message.read());
                    // Handle capabilities
                    final Capabilities.Builder builder = Capabilities.newBuilder();
                    for (;;) {
                        final int type = message.read();
                        if (type == -1) {
                            break;
                        }
                        // User capabilities are sent last.
                        if (type == USER_CAP) {
                            final Capabilities server = options.get(ConversationOptions.USER_CAPABILITIES, Capabilities.EMPTY);
                            processUserCapabilities(builder, server, message);
                            break;
                        }

                        final int len = readInt(message);
                        switch (type) {
                            case CAP_SINGLE_CONVERSATION:
                                singleConversationPerChannel = true;
                                break;
                            default:
                                message.skip(len); // Discard unknown data
                        }
                    }
                    message.close();

                    // We are going to use the client conversation id to send messages to the client
                    final Capabilities capabilities = builder.build();
                    ConversationHandlerImpl conversation;
                    for (;;) {
                        // The client is going to use our conversation ID when sending messages
                        final int conversationID = ThreadLocalRandom.current().nextInt();
                        conversation = new ConversationHandlerImpl(conversationID, clientConversationID, messageListener, channel, capabilities, executor);
                        if (conversations.putIfAbsent(conversation) == null) {
                            // Register the close handlers
                            conversationOpened(conversationID, conversation, channel);
                            // Send the conversation open acknowledgement
                            boolean ok = false;
                            final MessageOutputStream os = channel.writeMessage();
                            try {
                                writeInt(os, clientConversationID);
                                os.write(CONVERSATION_OPENED);
                                writeInt(os, conversationID);
                                os.write(CAP_VERSION);
                                os.write(version);

                                if (singleConversationPerChannel) {
                                    os.write(CAP_SINGLE_CONVERSATION);
                                    writeInt(os, 0);
                                }

                                // Write user capabilities
                                writeUserCapabilities(capabilities, os);

                                os.close();
                                ok = true;
                            } finally {
                                if (!ok) {
                                    conversation.closeAsync();
                                }
                                IoUtils.safeClose(os);
                                channel.release();
                            }
                            return;
                        }
                    }
                } catch (IOException e) {
                    sendProtocolError(clientConversationID, channel, "opening new conversation failed");
                }
            } else {
                try {
                    final OutputStream os = channel.writeMessage();
                    try {
                        writeInt(os, clientConversationID);
                        os.write(CONVERSATION_REJECTED);
                        os.close();
                    } finally {
                        IoUtils.safeClose(os);
                        channel.release();
                    }
                } catch (IOException e) {
                    ConversationLogger.ROOT_LOGGER.debugf(e, "failed to send rejected message");
                }
            }
        } finally {
            IoUtils.safeClose(message);
        }
    }


    protected void conversationOpened(final int conversationID, final ConversationHandlerImpl conversation, final Channel channel) throws IOException {
        // Add the close handler
        channel.addCloseHandler(conversation.getCloseHandler());
        conversation.addCloseHandler(new CloseHandler<Conversation>() {
            @Override
            public void handleClose(Conversation closed, IOException exception) {
                assert closed == conversation;
                conversationClosed(conversationID, conversation);
            }
        });
    }

    protected void conversationClosed(final int conversationID, final Conversation conversation) {
        if (conversations.remove(conversation)) {
            resourceFinished();
        } else {
            ConversationLogger.ROOT_LOGGER.debugf("failed to unregister conversation %s", conversationID);
        }
    }

    @Override
    protected void closeAction() throws IOException {
        prepareShutdown();
        final Iterator<ConversationHandlerImpl> i = conversations.iterator();
        while (i.hasNext()) {
            i.next().closeAsync();
        }
    }

    static ChannelWrapper getWrapper(final Channel channel, final OptionMap options) {
        ChannelWrapper wrapper = channel.getAttachments().getAttachment(ChannelWrapper.ATTACHMENT);
        if (wrapper == null) {
            wrapper = new ChannelWrapper(channel, options);
            channel.getAttachments().attach(ChannelWrapper.ATTACHMENT, wrapper);
        }
        return wrapper;
    }

}
