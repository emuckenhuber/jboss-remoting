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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.jboss.remoting3.Attachable;
import org.jboss.remoting3.HandleableCloseable;
import org.xnio.channels.Configurable;

/**
 * A conversation provides a basic request/respond correlation by defining a {@code MessageHandler} when writing a message.
 * Messages without a handler are considered one way messages and won't receive any response. Multiple messages may be
 * received concurrently on a conversation in no particular order. On the receiver side initial messages are handled
 * using the {@link ConversationMessageReceiver}, after that message handlers may be used to process multiple messages
 * in an exchange.
 *
 * @author Emanuel Muckenhuber
 */
public interface Conversation extends Attachable, Configurable, HandleableCloseable<Conversation> {

    /**
     * Write a new message, not expecting any response.
     *
     * @return the message output stream
     * @throws IOException
     */
    OutputStream writeMessage() throws IOException;

    /**
     * Write a new message with a given message handler used to process the response.
     *
     * @param messageHandler the response handler
     * @return the message output stream
     * @throws IOException
     */
    OutputStream writeMessage(MessageHandler messageHandler) throws IOException;

    /**
     * Handle an error condition on the conversation, this will close the conversion and cancel all pending requests.
     *
     * @param e the error
     */
    void handleError(IOException e);

    public interface MessageHandler {

        /**
         * Handle an incoming message.
         *
         * @param conversation the associated conversation
         * @param message      the incoming message
         * @throws java.io.IOException
         */
        void handleMessage(Conversation conversation, InputStream message) throws IOException;

        /**
         * Handle an error condition.
         *
         * @param conversation the associated conversation
         * @param e the error
         */
        void handleError(Conversation conversation, IOException e);

        /**
         * Handle cancellation - when the conversation gets closed before the response was received.
         *
         * @param conversation the associated conversation
         */
        void handleCancellation(Conversation conversation);

    }
}
