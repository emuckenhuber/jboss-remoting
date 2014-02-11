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

package org.jboss.remoting3.conversation.test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.CountDownLatch;

import org.jboss.remoting3.conversation.Conversation;
import org.jboss.remoting3.conversation.ConversationChannelClient;
import org.jboss.remoting3.conversation.ConversationChannelHandler;
import org.jboss.remoting3.conversation.ConversationMessageReceiver;
import org.jboss.remoting3.conversation.Conversations;
import org.junit.Assert;
import org.junit.Test;
import org.xnio.FutureResult;
import org.xnio.IoFuture;
import org.xnio.IoUtils;
import org.xnio.OptionMap;

/**
 * @author Emanuel Muckenhuber
 */
public class BasicConversationUnitTestCase extends AbstractConversationTestBase {

    // Nested conversation handler
    static Conversation.MessageHandler NESTED_HANDLER = new Conversations.AbstractMessageHandler() {
        @Override
        protected void doHandleMessage(Conversation conversation, InputStream message) throws IOException {
            final int response = message.read();
            if (response == 0x11) {
                // End current message exchange after this
                final OutputStream os = conversation.writeMessage();
                try {
                    os.write(0x10);
                    os.close();
                } finally {
                    IoUtils.safeClose(os);
                }
            } else {
                throw new IOException();
            }
        }
    };

    static ConversationMessageReceiver RECEIVER = new Conversations.AbstractConversationMessageReceiver() {

        @Override
        protected void doHandleMessage(Conversation conversation, InputStream message) throws IOException {
            final int type = message.read();
            if (type == BasicTestProtocol.SIMPLE) {
                final OutputStream os = conversation.writeMessage();
                try {
                    os.write(0x12);
                    os.close();
                } finally {
                    IoUtils.safeClose(os);
                }
            } else if (type == BasicTestProtocol.NESTED) {
                final OutputStream os = conversation.writeMessage(NESTED_HANDLER);
                try {
                    os.write(0x12);
                    os.close();
                } finally {
                    IoUtils.safeClose(os);
                }
            } else {
                throw new IOException();
            }
        }
    };


    @Test
    public void testSimple() throws IOException {
        // Setup the server
        final ConversationChannelHandler server = Conversations.createServerReceiver(RECEIVER, executor);
        recvChannel.receiveMessage(server.getReceiver());

        // Create the client and send a simple request
        final ConversationChannelClient client = Conversations.createClient(sendChannel, executor);
        try {
            final Conversation conversation = client.openConversation(OptionMap.EMPTY).get();
            try {
                final CountDownLatch latch = new CountDownLatch(1);
                // Create the response handler
                final OutputStream os = conversation.writeMessage(new Conversations.AbstractMessageHandler() {
                    @Override
                    protected void doHandleMessage(Conversation conversation, InputStream message) throws IOException {
                        try {
                            message.read();
                        } finally {
                            latch.countDown();
                        }
                    }
                });
                try {
                    // Send the actual message
                    os.write(BasicTestProtocol.SIMPLE);
                    os.close();
                } finally {
                    IoUtils.safeClose(os);
                }
                try {
                    latch.await();
                    conversation.close();
                } catch (InterruptedException e) {
                    //
                }
            } finally {
                IoUtils.safeClose(conversation);
            }
        } finally {
            IoUtils.safeClose(client);
        }
    }

    @Test
    public void testSimpleFailure() throws IOException {
        // Setup the server
        final ConversationChannelHandler server = Conversations.createServerReceiver(RECEIVER, executor);
        recvChannel.receiveMessage(server.getReceiver());

        // Create the client and send a simple request
        final ConversationChannelClient client = Conversations.createClient(sendChannel, executor);
        try {
            final Conversation conversation = client.openConversation(OptionMap.EMPTY).get();
            try {
                final CountDownLatch latch = new CountDownLatch(1);
                // Create the response handler
                final OutputStream os = conversation.writeMessage(new Conversations.AbstractMessageHandler() {

                    @Override
                    public void handleError(Conversation conversation, IOException e) {
                        latch.countDown();
                    }

                    @Override
                    protected void doHandleMessage(Conversation conversation, InputStream message) throws IOException {
                        //
                    }
                });
                try {
                    os.write(BasicTestProtocol.FAILURE);
                    os.close();
                } finally {
                    IoUtils.safeClose(os);
                }
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    return;
                }
            } finally {
                IoUtils.safeClose(conversation);
            }
        } finally {
            IoUtils.safeClose(client);
        }
    }


    @Test
    public void testNested() throws IOException {
        // Setup the server
        final ConversationChannelHandler server = Conversations.createServerReceiver(RECEIVER, executor);
        recvChannel.receiveMessage(server.getReceiver());

        // Create the client and send a simple request
        final ConversationChannelClient client = Conversations.createClient(sendChannel, executor);
        try {
            final Conversation conversation = client.openConversation(OptionMap.EMPTY).get();
            try {
                final ResultMessageHandler handler = new ResultMessageHandler(false);
                final IoFuture<Boolean> future = handler.getFuture();
                final OutputStream os = conversation.writeMessage(handler);
                try {
                    os.write(BasicTestProtocol.NESTED);
                    os.close();
                } finally {
                    IoUtils.safeClose(os);
                }

                //
                Assert.assertTrue(future.get());

            } finally {
                IoUtils.safeClose(conversation);
            }
        } finally {
            IoUtils.safeClose(client);
        }
    }

    @Test
    public void testNestedFailure() throws IOException {
        // Setup the server
        final ConversationChannelHandler server = Conversations.createServerReceiver(RECEIVER, executor);
        recvChannel.receiveMessage(server.getReceiver());

        // Create the client and send a simple request
        final ConversationChannelClient client = Conversations.createClient(sendChannel, executor);
        try {
            final Conversation conversation = client.openConversation(OptionMap.EMPTY).get();
            try {
                // Send the failure byte
                final ResultMessageHandler handler = new ResultMessageHandler(true);
                final IoFuture<Boolean> future = handler.getFuture();
                final OutputStream os = conversation.writeMessage(handler);
                try {
                    os.write(BasicTestProtocol.NESTED);
                    os.close();
                } finally {
                    IoUtils.safeClose(os);
                }

                //
                future.await();
                Assert.assertEquals(IoFuture.Status.FAILED, future.getStatus());

            } finally {
                IoUtils.safeClose(conversation);
            }
        } finally {
            IoUtils.safeClose(client);
        }
    }

    // Client message handler for nested message exchange
    protected static class ResultMessageHandler implements Conversation.MessageHandler {

        private final boolean sendFailure;
        private final FutureResult<Boolean> result = new FutureResult<Boolean>();

        public ResultMessageHandler(boolean sendFailure) {
            this.sendFailure = sendFailure;
        }

        IoFuture<Boolean> getFuture() {
            return result.getIoFuture();
        }

        @Override
        public void handleMessage(Conversation conversation, InputStream message) throws IOException {
            try {
                final int response = message.read();
                message.close();
                final byte request;
                if (sendFailure) {
                    request = BasicTestProtocol.FAILURE;
                } else if (response == 0x12) {
                    request = 0x11;
                } else if (response == 0x10) {
                    // We are done
                    result.setResult(Boolean.TRUE);
                    return;
                } else {
                    throw new IOException();
                }

                // Send a new message
                final OutputStream os = conversation.writeMessage(this);
                os.write(request);
                os.close();

            } finally {
                IoUtils.safeClose(message);
            }
        }

        @Override
        public void handleError(Conversation conversation, IOException e) {
            result.setException(e);
            conversation.handleError(e);
        }

        @Override
        public void handleCancellation(Conversation conversation) {
            result.setCancelled();
        }
    }

}
