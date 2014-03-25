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

import static org.jboss.remoting3.conversation.Protocol.CONVERSATION_CLOSED;
import static org.jboss.remoting3.conversation.Protocol.CONVERSATION_MESSAGE;
import static org.jboss.remoting3.conversation.Protocol.CONVERSATION_OPENED;
import static org.jboss.remoting3.conversation.Protocol.CONVERSATION_REJECTED;
import static org.jboss.remoting3.conversation.Protocol.OPEN_CONVERSATION;
import static org.jboss.remoting3.conversation.Protocol.PROTOCOL_ERROR;
import static org.jboss.remoting3.conversation.ProtocolUtils.sendProtocolError;
import static org.jboss.remoting3.conversation.ProtocolUtils.writeInt;
import static org.xnio.IoUtils.safeClose;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.Executor;

import org.jboss.remoting3.Channel;
import org.jboss.remoting3.HandleableCloseable;
import org.jboss.remoting3.MessageInputStream;
import org.xnio.IoUtils;

/**
 * @author Emanuel Muckenhuber
 */
abstract class ConversationProtocolHandler<T extends HandleableCloseable<T>> extends ConversationResourceTracker<T> {

    protected ConversationProtocolHandler(Executor executor) {
        super(executor);
    }

    abstract ConversationProtocolHandler getProtocolHandler(final int conversationID);

    void openConversation(int conversationID, Channel channel, MessageInputStream message) throws IOException {
        try {
            final OutputStream os = channel.writeMessage();
            boolean ok = false;
            try {
                writeInt(os, conversationID);
                os.write(CONVERSATION_REJECTED);
                os.close();
            } finally {
                if (!ok) {
                    safeClose(os);
                }
            }
        } finally {
            safeClose(message);
        }
    }

    void conversationOpenend(MessageInputStream message) throws IOException {
        handleError(message, new IOException());
    }

    void conversationRejected(MessageInputStream message) throws IOException {
        handleError(message, new IOException());
    }

    void handleMessage(MessageInputStream message) throws IOException {
        handleError(message, new IOException());
    }

    void conversationClosed(MessageInputStream message) throws IOException {
        handleError(message, new IOException());
    }

    void handleProtocolError(MessageInputStream message) throws IOException {
        handleError(message, new IOException());
    }

    void handleError(final MessageInputStream message, final IOException error) throws IOException {
        IoUtils.safeClose(message); // discard message
        throw error;
    }

    protected void handle(int conversationID, int messageType, Channel channel, MessageInputStream message) throws IOException {
        final ConversationProtocolHandler handler = getProtocolHandler(conversationID);
        switch (messageType) {
            case OPEN_CONVERSATION:
                openConversation(conversationID, channel, message);
                break;
            case CONVERSATION_OPENED:
                if (handler != null) {
                    handler.conversationOpenend(message);
                    break;
                }
            case CONVERSATION_REJECTED:
                if (handler != null) {
                    handler.conversationRejected(message);
                    break;
                }
            case CONVERSATION_MESSAGE:
                if (handler != null) {
                    handler.handleMessage(message);
                    break;
                }
            case CONVERSATION_CLOSED:
                if (handler != null) {
                    handler.conversationClosed(message);
                    break;
                }
                sendProtocolError(conversationID, channel, "no registered conversation");
                break;
            case PROTOCOL_ERROR:
                if (handler != null) {
                    handler.handleProtocolError(message);
                } else {
                    ConversationLogger.ROOT_LOGGER.debugf("protocol error for conversation %s", conversationID);
                }
                break;
            default:
                sendProtocolError(conversationID, channel, "invalid protocol message type");
                break;
        }
    }

}
