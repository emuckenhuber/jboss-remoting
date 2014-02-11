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
import java.io.InterruptedIOException;
import java.util.concurrent.Semaphore;

import org.jboss.remoting3.Attachments;
import org.jboss.remoting3.Channel;
import org.jboss.remoting3.ChannelBusyException;
import org.jboss.remoting3.CloseHandler;
import org.jboss.remoting3.Connection;
import org.jboss.remoting3.MessageOutputStream;
import org.jboss.remoting3.RemotingOptions;
import org.xnio.Option;
import org.xnio.OptionMap;

/**
 * @author Emanuel Muckenhuber
 */
class ChannelWrapper implements Channel {

    static final Attachments.Key<ChannelWrapper> ATTACHMENT = new Attachments.Key(ChannelWrapper.class);

    private final Channel channel;
    private final Semaphore semaphore;
    private final boolean checkCurrentThread;

    public ChannelWrapper(Channel delegate, OptionMap options) {
        this.channel = delegate;
        this.checkCurrentThread = !options.get(ConversationOptions.BLOCK_RECEIVING_THREAD, false);
        Integer maxOutboundMessages = channel.getOption(RemotingOptions.MAX_OUTBOUND_MESSAGES);
        if (maxOutboundMessages == null) {
            // Use the default for the remote inbound connections
            maxOutboundMessages = RemotingOptions.INCOMING_CHANNEL_DEFAULT_MAX_OUTBOUND_MESSAGES;
        }
        this.semaphore = new Semaphore(maxOutboundMessages, true);
    }

    @Override
    public boolean supportsOption(Option<?> option) {
        return channel.supportsOption(option);
    }

    @Override
    public <T> T getOption(Option<T> option) {
        return channel.getOption(option);
    }

    @Override
    public <T> T setOption(Option<T> option, T value) throws IllegalArgumentException {
        return channel.setOption(option, value);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    @Override
    public Attachments getAttachments() {
        return channel.getAttachments();
    }

    @Override
    public void awaitClosed() throws InterruptedException {
        channel.awaitClosed();
    }

    @Override
    public void awaitClosedUninterruptibly() {
        channel.awaitClosedUninterruptibly();
    }

    @Override
    public void closeAsync() {
        channel.closeAsync();
    }

    @Override
    public Key addCloseHandler(CloseHandler<? super Channel> handler) {
        return channel.addCloseHandler(handler);
    }

    @Override
    public Connection getConnection() {
        return channel.getConnection();
    }

    @Override
    public MessageOutputStream writeMessage() throws IOException {
        return writeMessageChecked(null);
    }

    @Override
    public void writeShutdown() throws IOException {
        channel.writeShutdown();
    }

    @Override
    public void receiveMessage(Receiver handler) {
        channel.receiveMessage(handler);
    }

    protected MessageOutputStream writeMessageChecked(final Thread reference) throws IOException {
        if (checkCurrentThread && Thread.currentThread() == reference) {
            if (!semaphore.tryAcquire()) {
                throw new ChannelBusyException("Too many open outbound writes");
            }
        } else {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }
        }
        boolean ok = false;
        try {
            final MessageOutputStream os = channel.writeMessage();
            ok = true;
            return os;
        } finally {
            if (!ok) {
                semaphore.release();
            }
        }
    }

    protected void release() {
        semaphore.release();
    }

}
