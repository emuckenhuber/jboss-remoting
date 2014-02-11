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
import java.io.OutputStream;

import org.jboss.remoting3.Attachments;
import org.jboss.remoting3.CloseHandler;
import org.xnio.Option;

/**
 * Conversation wrapper, representing a one-way message. This type does not support any response.
 *
 * @author Emanuel Muckenhuber
 */
public class OneWayConversationResponse implements Conversation {

    private final Conversation conversation;
    private final Thread thread;

    public OneWayConversationResponse(Conversation conversation) {
        this.conversation = conversation;
        this.thread = Thread.currentThread();
    }

    @Override
    public OutputStream writeMessage() throws IOException {
        throw new IOException("remote side does not expect response");
    }

    @Override
    public OutputStream writeMessage(MessageHandler messageHandler) throws IOException {
        throw new IOException("remote side does not expect a response");
    }

    @Override
    public void handleError(IOException e) {
        conversation.handleError(e);
    }

    @Override
    public Attachments getAttachments() {
        return conversation.getAttachments();
    }

    @Override
    public boolean supportsOption(Option<?> option) {
        return conversation.supportsOption(option);
    }

    @Override
    public <T> T getOption(Option<T> option) throws IOException {
        return conversation.getOption(option);
    }

    @Override
    public <T> T setOption(Option<T> option, T value) throws IllegalArgumentException, IOException {
        return conversation.setOption(option, value);
    }

    @Override
    public boolean hasCapability(Capability<?> capability) {
        return conversation.hasCapability(capability);
    }

    @Override
    public <T> boolean supportsCapability(Capability<T> capability, T value) {
        return conversation.supportsCapability(capability, value);
    }

    @Override
    public <T> T getCapability(Capability<T> capability) {
        return conversation.getCapability(capability);
    }

    @Override
    public void close() throws IOException {
        if (Thread.currentThread() == thread) {
            conversation.closeAsync();
        } else {
            conversation.close();
        }
    }

    @Override
    public void awaitClosed() throws InterruptedException {
        conversation.awaitClosed();
    }

    @Override
    public void awaitClosedUninterruptibly() {
        conversation.awaitClosedUninterruptibly();
    }

    @Override
    public void closeAsync() {
        conversation.closeAsync();
    }

    @Override
    public Key addCloseHandler(CloseHandler<? super Conversation> handler) {
        return conversation.addCloseHandler(handler);
    }

}
