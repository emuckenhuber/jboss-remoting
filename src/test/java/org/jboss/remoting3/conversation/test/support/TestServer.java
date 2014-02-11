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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.jboss.remoting3.Channel;
import org.jboss.remoting3.CloseHandler;
import org.jboss.remoting3.OpenListener;
import org.jboss.remoting3.conversation.ConversationChannelHandler;
import org.jboss.remoting3.conversation.ConversationMessageReceiver;
import org.jboss.remoting3.conversation.Conversations;
import org.xnio.IoUtils;
import org.xnio.OptionMap;

/**
 * @author Emanuel Muckenhuber
 */
public class TestServer implements OpenListener {

    private final ExecutorService executor;
    private final ConversationChannelHandler receiver;
    private final CountDownLatch latch = new CountDownLatch(1);

    public TestServer(ConversationChannelHandler receiver, ExecutorService executor) {
        this.receiver = receiver;
        this.executor = executor;
        receiver.addCloseHandler(new CloseHandler<ConversationChannelHandler>() {
            @Override
            public void handleClose(ConversationChannelHandler closed, IOException exception) {
                latch.countDown();
            }
        });
    }

    @Override
    public void channelOpened(Channel channel) {
        channel.receiveMessage(receiver.getReceiver());
    }

    @Override
    public void registrationTerminated() {
        shutdownNow();
    }

    public void shutdown() {
        receiver.prepareShutdown();
    }

    boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return latch.await(timeout, unit);
    }

    public void shutdownNow() {
        try {
            IoUtils.safeClose(receiver);
        } finally {
            executor.shutdownNow();
        }
    }

    static TestServer create(final ConversationMessageReceiver messageReceiver, final OptionMap options) {
        final ExecutorService executorService = Executors.newCachedThreadPool();
        final ConversationChannelHandler receiver = Conversations.createServerReceiver(messageReceiver, options, executorService);
        return new TestServer(receiver, executorService);
    }

}
