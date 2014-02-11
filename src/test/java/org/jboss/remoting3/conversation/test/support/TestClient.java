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

package org.jboss.remoting3.conversation.test.support;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.jboss.remoting3.CloseHandler;
import org.jboss.remoting3.conversation.Conversation;
import org.jboss.remoting3.conversation.ConversationChannelClient;
import org.xnio.FutureResult;
import org.xnio.IoFuture;
import org.xnio.IoUtils;
import org.xnio.OptionMap;

/**
 * @author Emanuel Muckenhuber
 */
public class TestClient {

    private final ExecutorService clientExecutor;
    private final ConversationChannelClient client;
    private final CountDownLatch latch = new CountDownLatch(1);

    public TestClient(ConversationChannelClient client, ExecutorService clientExecutor) {
        this.client = client;
        this.clientExecutor = clientExecutor;
        client.addCloseHandler(new CloseHandler<ConversationChannelClient>() {
            @Override
            public void handleClose(ConversationChannelClient closed, IOException exception) {
                latch.countDown();
            }
        });
    }

    public <T> TestResult<T> execute(Class<T> clazz, OptionMap options) throws IOException {
        final FutureResult<T> result = new FutureResult<T>();
        final IoFuture<Conversation> conversationIoFuture = client.openConversation(options);
        conversationIoFuture.addNotifier(new IoFuture.HandlingNotifier<Conversation, Void>() {
            @Override
            public void handleCancelled(Void attachment) {
                result.setCancelled();
            }

            @Override
            public void handleFailed(IOException exception, Void attachment) {
                result.setException(exception);
            }

            @Override
            public void handleDone(Conversation conversation, Void attachment) {
                try {
                    final OutputStream os = conversation.writeMessage();

                } catch (IOException e) {
                    result.setException(e);
                }
            }
        }, null);
        return new TestResult<T>(result.getIoFuture());
    }

    public void shutdown() {
        client.prepareShutdown();
    }

    boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return latch.await(timeout, unit);
    }

    public void shutdownNow() {
        try {
            IoUtils.safeClose(client);
        } finally {
            clientExecutor.shutdownNow();
        }
    }

}
