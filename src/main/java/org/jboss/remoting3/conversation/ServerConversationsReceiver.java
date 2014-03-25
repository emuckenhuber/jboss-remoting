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
import static org.jboss.remoting3.conversation.Protocol.CONVERSATION_CLOSED;
import static org.jboss.remoting3.conversation.Protocol.CONVERSATION_MESSAGE;
import static org.jboss.remoting3.conversation.Protocol.CONVERSATION_OPENED;
import static org.jboss.remoting3.conversation.Protocol.CONVERSATION_REJECTED;
import static org.jboss.remoting3.conversation.Protocol.OPEN_CONVERSATION;
import static org.jboss.remoting3.conversation.Protocol.PROTOCOL_ERROR;
import static org.jboss.remoting3.conversation.Protocol.VERSION;
import static org.jboss.remoting3.conversation.ProtocolUtils.readInt;
import static org.jboss.remoting3.conversation.ProtocolUtils.sendProtocolError;
import static org.jboss.remoting3.conversation.ProtocolUtils.writeInt;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.remoting3.Channel;
import org.jboss.remoting3.CloseHandler;
import org.jboss.remoting3.MessageInputStream;
import org.jboss.remoting3.MessageOutputStream;
import org.xnio.IoUtils;

/**
 * @author Emanuel Muckenhuber
 */
class ServerConversationsReceiver extends ConversationProtocolHandler<ServerConversationsReceiver> implements Channel.Receiver {

    private final Executor executor;
    private final ConversationMessageReceiver messageListener;
    private final AtomicInteger conversationIDs = new AtomicInteger();
    private final ConcurrentMap<Integer, NewConversationImpl> conversations = new ConcurrentHashMap<Integer, NewConversationImpl>();

    ServerConversationsReceiver(ConversationMessageReceiver messageListener, Executor executor) {
        super(executor);
        this.executor = executor;
        this.messageListener = messageListener;
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
    ConversationProtocolHandler getProtocolHandler(int conversationID) {
        return conversations.get(conversationID);
    }

    @Override
    public void handleMessage(final Channel channel, final MessageInputStream message) {
        try {
            final int conversationID = readInt(message);
            final int messageType = message.read();
            handle(conversationID, messageType, channel, message);
            System.out.println("received " + conversationID);
            channel.receiveMessage(this);
        } catch (IOException e) {
            handleError(channel, e);
        }
    }

    @Override
    protected void openConversation(final int clientConversationID, final Channel channel, final MessageInputStream message) {
        try {
            if (incrementResourceCountUnchecked()) {
                try {
                    // Conversation negotiation
                    ProtocolUtils.expectHeader(message, CAP_VERSION);
                    final int version = Math.min(VERSION, message.read());
                    // TODO additional capabilities negotiation

                    //
                    message.close();

                    // We are going to use the client conversation id to send messages to the client
                    final NewConversationImpl conversation = new NewConversationImpl(clientConversationID, messageListener, channel, executor);
                    for (;;) {
                        // The client is going to use our conversation ID when sending messages
                        final int conversationID = conversationIDs.incrementAndGet();
                        if (!conversations.containsKey(conversationID)) {
                            if (conversations.putIfAbsent(conversationID, conversation) == null) {
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
                                    // TODO capabilities
                                    os.close();
                                    ok = true;
                                } finally {
                                    if (!ok) {
                                        conversation.closeAsync();
                                    }
                                    IoUtils.safeClose(os);
                                }
                                return;
                            }
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
                    }
                } catch (IOException e) {
                    ConversationLogger.ROOT_LOGGER.debugf(e, "failed to send rejected message");
                }
            }
        } finally {
            IoUtils.safeClose(message);
        }
    }

    protected void conversationOpened(final int conversationID, final NewConversationImpl conversation, final Channel channel) throws IOException {
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
        if (conversations.remove(conversationID, conversation)) {
            resourceFinished();
        } else {
            ConversationLogger.ROOT_LOGGER.debugf("failed to unregister conversation %s", conversationID);
        }
    }

    @Override
    protected void closeAction() throws IOException {
        prepareClose();
        for (final Conversation conversation : conversations.values()) {
            conversation.closeAsync();
        }
    }

}
