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
import java.util.concurrent.Executor;

import org.jboss.remoting3.Channel;
import org.xnio.IoUtils;

/**
 * @author Emanuel Muckenhuber
 */
public final class Conversations {

    private Conversations() {
        //
    }

    /**
     * Create a client. This will register a channel receiver and exclusively handle all incoming messages.
     *
     * @param channel  the channel
     * @param executor the executor
     * @return the conversation client
     */
    public static ConversationChannelClient createClient(final Channel channel, final Executor executor) {
        final NewClientThing client = new NewClientThing(channel, null, executor);
        channel.receiveMessage(client);
        return client;
    }

    /**
     * Create a server channel receiver.
     *
     * @param receiver the conversation message receiver
     * @param executor the executor
     * @return the server receiver
     */
    public static Channel.Receiver createServerReceiver(final ConversationMessageReceiver receiver, final Executor executor) {
        // TODO this probably should return something else, an open listener?
        return new ServerConversationsReceiver(receiver, executor);
    }

    // basic message handler making sure the streams are closed!
    public abstract static class AbstractMessageHandler implements Conversation.MessageHandler {

        protected abstract void doHandleMessage(Conversation conversation, InputStream message) throws IOException;

        @Override
        public void handleMessage(Conversation conversation, InputStream message) throws IOException {
            try {
                // Handle the message and make sure that the input stream gets closed, always!
                doHandleMessage(conversation, message);
                message.close();
            } finally {
                IoUtils.safeClose(message);
            }
        }

        @Override
        public void handleError(Conversation conversation, IOException e) {
            conversation.handleError(e);
        }

        @Override
        public void handleCancellation(Conversation conversation) {
            // We don't maintain state
        }

    }

    // basic message receiver making sure the streams are closed
    public abstract static class AbstractConversationMessageReceiver implements ConversationMessageReceiver {

        protected abstract void doHandleMessage(Conversation conversation, InputStream message) throws IOException;

        @Override
        public void handleMessage(Conversation conversation, InputStream message) throws IOException {
            try {
                doHandleMessage(conversation, message);
                message.close();
            } finally {
                IoUtils.safeClose(message);
            }
        }
    }
}
