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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jboss.remoting3.conversation.Conversation;
import org.jboss.remoting3.conversation.ConversationChannelClient;
import org.jboss.remoting3.conversation.ConversationChannelHandler;
import org.jboss.remoting3.conversation.ConversationMessageReceiver;
import org.jboss.remoting3.conversation.Conversations;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xnio.IoUtils;
import org.xnio.OptionMap;

/**
 * @author Emanuel Muckenhuber
 */
public class SimpleEchoUnitTestCase extends AbstractConversationTestBase {

    private static final int size = 0x10000;
    private static final byte[] MESSAGE = new byte[size];
    private static final int NUMBER_OF_CONVERSATIONS = 100;
    private static final int NUMBER_OF_MESSAGES = 10;

    private ExecutorService clientExecutor;
    private ExecutorService taskExecutor;

    static {
        new Random().nextBytes(MESSAGE);
    }

    @Before
    public void setUp() throws Exception {
        clientExecutor = Executors.newFixedThreadPool(5);
        taskExecutor = Executors.newFixedThreadPool(40);
    }

    @After
    public void tearDown() throws Exception {
        taskExecutor.shutdownNow();
        clientExecutor.shutdownNow();
    }

    private static final Conversation.MessageHandler HANDLER = new Conversations.AbstractMessageHandler() {
        @Override
        protected void doHandleMessage(final Conversation conversation, InputStream message) throws IOException {
            final int i = readInt(message);
            final byte[] data = readData(message, size);
            message.close();

            final WriteTask task = new WriteTask(i, data, conversation, this);
            executor.execute(task);
        }
    };

    private static final ConversationMessageReceiver RECEIVER = new ConversationMessageReceiver() {
        @Override
        public void handleMessage(Conversation conversation, InputStream message) throws IOException {
            HANDLER.handleMessage(conversation, message);
        }
    };

    @Test
    public void test() throws IOException, InterruptedException, ExecutionException {

        // Setup the server
        final ConversationChannelHandler server = Conversations.createServerReceiver(RECEIVER, executor);
        recvChannel.receiveMessage(server.getReceiver());

        // Create the client and send a simple request
        final ConversationChannelClient client = Conversations.createClient(sendChannel, executor);
        try {
            final CountDownLatch latch = new CountDownLatch(NUMBER_OF_CONVERSATIONS);
            for (int i = 0; i < NUMBER_OF_CONVERSATIONS; i++) {
                final ConversationTask task = new ConversationTask(client, latch);
                clientExecutor.execute(task);
            }
            latch.await();
        } finally {
            IoUtils.safeClose(client);
        }
    }

    static class ConversationTask implements Runnable {

        private final ConversationChannelClient client;
        private final ClientHandler handler;
        ConversationTask(ConversationChannelClient client, CountDownLatch latch) {
            this.client = client;
            this.handler = new ClientHandler(latch);
        }

        @Override
        public void run() {
            try {
                final Conversation conversation = client.openConversation(OptionMap.EMPTY).get();
                final WriteTask task = new WriteTask(0, MESSAGE, conversation, handler);
                task.run();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    static class ClientHandler extends Conversations.AbstractMessageHandler {

        private final CountDownLatch latch;
        ClientHandler(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        protected void doHandleMessage(Conversation conversation, InputStream message) throws IOException {

            final int i = readInt(message);
            final byte[] data = readData(message, size);
            message.close();

            if (i == NUMBER_OF_MESSAGES) {
                latch.countDown();
                conversation.close();
            } else {
                final WriteTask task = new WriteTask(i, data, conversation, this);
                executor.execute(task);
            }
        }
    }

    static class WriteTask implements Runnable {

        private final int i;
        private final byte[] data;
        private final Conversation conversation;
        private final Conversation.MessageHandler handler;

        WriteTask(int i, byte[] data, Conversation conversation, Conversation.MessageHandler handler) {
            this.i = i + 1;
            this.data = data;
            this.conversation = conversation;
            this.handler = handler;
        }

        @Override
        public void run() {
            try {
                final OutputStream os;
                if (i == NUMBER_OF_MESSAGES) {
                    os = conversation.writeMessage();
                } else {
                    os = conversation.writeMessage(handler);
                }
                try {
                    writeInt(os, i);
                    os.write(data);
                    os.close();
                } finally {
                    IoUtils.safeClose(os);
                }
            } catch (IOException e) {
                conversation.handleError(e);
            }
        }
    }

    static int readInt(final InputStream in) throws IOException {
        int ch1 = in.read();
        int ch2 = in.read();
        int ch3 = in.read();
        int ch4 = in.read();
        if ((ch1 | ch2 | ch3 | ch4) < 0)
            throw new EOFException();
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }

    static void writeInt(OutputStream out, int v) throws IOException {
        out.write((v >>> 24) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>>  8) & 0xFF);
        out.write((v >>>  0) & 0xFF);
    }

    static byte[] readData(final InputStream is, final int messageSize) throws IOException {
        int c;
        int pos = 0;
        int remaining = messageSize;
        final byte[] data = new byte[messageSize];
        while (remaining > 0 && (c = is.read(data, pos, remaining)) != -1) {
            pos = pos + c;
            remaining = remaining - c;
        }
        return data;
    }

}
