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
import static org.jboss.remoting3.conversation.Protocol.MESSAGE_BODY;
import static org.jboss.remoting3.conversation.Protocol.MESSAGE_FAILED;
import static org.jboss.remoting3.conversation.Protocol.NEW_REQUEST_MESSAGE;
import static org.jboss.remoting3.conversation.Protocol.REQUEST_ID;
import static org.jboss.remoting3.conversation.Protocol.RESPONSE_MESSAGE;
import static org.jboss.remoting3.conversation.ProtocolUtils.expectHeader;
import static org.jboss.remoting3.conversation.ProtocolUtils.readInt;
import static org.jboss.remoting3.conversation.ProtocolUtils.sendProtocolError;
import static org.jboss.remoting3.conversation.ProtocolUtils.writeInt;

import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.remoting3.Attachments;
import org.jboss.remoting3.Channel;
import org.jboss.remoting3.CloseHandler;
import org.jboss.remoting3.MessageInputStream;
import org.jboss.remoting3.MessageOutputStream;
import org.xnio.Cancellable;
import org.xnio.IoUtils;
import org.xnio.Option;

/**
 * @author Emanuel Muckenhuber
 */
class NewConversationImpl extends ConversationProtocolHandler<Conversation> implements Conversation {

    private static final ConversationLogger log = ConversationLogger.ROOT_LOGGER;

    private final Channel channel;
    private final int conversationID;
    private final ConversationMessageReceiver messageReceiver;
    private final Attachments attachments = new Attachments();
    private final AtomicInteger requestIDs = new AtomicInteger(0); // request id generator
    private final Set<Cancellable> streams = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<Cancellable, Boolean>()));
    private final ConcurrentHashMap<Integer, PendingRequest> pendingRequests = new ConcurrentHashMap<Integer, PendingRequest>(16, 0.75f, Runtime.getRuntime().availableProcessors());
    private final CloseHandler<Channel> closeHandler = new CloseHandler<Channel>() {
        @Override
        public void handleClose(Channel closed, IOException exception) {
            handleRemoteClose();
        }
    };

    public NewConversationImpl(final int conversationID, final ConversationMessageReceiver messageReceiver, final Channel channel, final Executor executor) {
        super(executor);
        this.channel = channel;
        this.messageReceiver = messageReceiver;
        this.conversationID = conversationID;
    }

    @Override
    ConversationProtocolHandler getProtocolHandler(int conversationID) {
        return this;
    }

    CloseHandler<Channel> getCloseHandler() {
        return closeHandler;
    }

    @Override
    public Attachments getAttachments() {
        return attachments;
    }

    @Override
    public boolean supportsOption(Option<?> option) {
        return channel.supportsOption(option);
    }

    @Override
    public <T> T getOption(Option<T> option) throws IOException {
        return channel.getOption(option);
    }

    @Override
    public <T> T setOption(Option<T> option, T value) throws IllegalArgumentException, IOException {
        return channel.setOption(option, value);
    }

    @Override
    void conversationClosed(MessageInputStream message) throws IOException {
        try {
            IoUtils.safeClose(message);
        } finally {
            handleRemoteClose();
        }
    }

    @Override
    public OutputStream writeMessage() throws IOException {
        boolean ok = false;
        final OutputStream os = writeMessageHeader();
        try {
            os.write(NEW_REQUEST_MESSAGE);
            os.write(MESSAGE_BODY);
            ok = true;
            return os;
        } finally {
            if (!ok) {
                IoUtils.safeClose(os);
            }
        }
    }

    @Override
    public OutputStream writeMessage(Conversation.MessageHandler messageHandler) throws IOException {
        final int requestID = newPendingRequest(messageHandler);

        boolean ok = false;
        final OutputStream os = writeMessageHeader();
        try {
            os.write(NEW_REQUEST_MESSAGE);
            os.write(REQUEST_ID);
            writeInt(os, requestID);
            os.write(MESSAGE_BODY);
            ok = true;
            return os;
        } catch (IOException e) {
            handleFailure(requestID, this, e);
            throw e;
        } finally {
            if (!ok) {
                IoUtils.safeClose(os);
            }
        }
    }

    /**
     * Process incoming messages associated with this conversation.
     *
     * @param conversationID the conversation id
     * @param channel        the remoting channel
     * @param message        the message input stream
     * @throws IOException
     */
    @Override
    protected void handleMessage(final MessageInputStream message) throws IOException {
        final int type = message.read();
        if (type == NEW_REQUEST_MESSAGE) {
            final int requestType = message.read();
            final Conversation conversation;
            if (requestType == REQUEST_ID) { // Expects a response
                final int responseID = readInt(message);
                expectHeader(message, MESSAGE_BODY);
                conversation = new ConversationResponseImpl(this, responseID);
            } else if (requestType == MESSAGE_BODY) { // One way message
                conversation = this;
            } else {
                IoUtils.safeClose(message);
                sendProtocolError(conversationID, channel, "unknown request type");
                return;
            }
            incrementResourceCount(); // TODO handle closed
            final WrappedInputStream is = new WrappedInputStream(message);
            streams.add(is);
            boolean ok = false;
            try {
                messageReceiver.handleMessage(conversation, is);
                ok = true;
            } catch (IOException e) {
                conversation.handleError(e);
            } finally {
                if (!ok) {
                    IoUtils.safeClose(is);
                }
            }
        } else if (type == RESPONSE_MESSAGE) {
            final int requestID = readInt(message);
            final PendingRequest request = pendingRequests.remove(requestID);
            if (request == null) { // send error
                log.errorf("no message handler for request %s", requestID);
                IoUtils.safeClose(message);
                return;
            }
            final int responseType = message.read();
            if (responseType == MESSAGE_FAILED) {
                IoUtils.safeClose(message);
                final String error = "message failed";  // read error message
                request.handleError(this, new IOException(error));
                return;
            } else {
                final Conversation conversation;
                if (responseType == REQUEST_ID) { // Expects a response
                    final int responseID = readInt(message);
                    expectHeader(message, MESSAGE_BODY);
                    conversation = new ConversationResponseImpl(this, responseID);
                } else if (responseType == MESSAGE_BODY) { // One way
                    conversation = this;
                } else {
                    IoUtils.safeClose(message);
                    throw new IOException();
                }
                incrementResourceCount(); // TODO handle closed
                final WrappedInputStream is = new WrappedInputStream(message);
                streams.add(is);
                boolean ok = false;
                try {
                    request.handleMessage(conversation, is);
                    ok = true;
                } finally {
                    if (!ok) {
                        IoUtils.safeClose(is);
                    }
                }
            }
        }
    }

    // Create a new pending request
    protected int newPendingRequest(final MessageHandler messageHandler) throws IOException {
        incrementResourceCount();
        final PendingRequest request = new PendingRequest(messageHandler);
        int requestID;
        for (;;) {
            requestID = requestIDs.incrementAndGet();
            if (pendingRequests.putIfAbsent(requestID, request) == null) {
                break;
            }
        }
        return requestID;
    }

    // Handle immediate local failures
    protected void handleFailure(final int requestID, Conversation conversation, final IOException e) {
        final PendingRequest request = pendingRequests.remove(requestID);
        if (request != null) {
            request.handleError(conversation, e);
        }
    }

    // Write the message header, increase the resource count and wrap the output stream
    protected OutputStream writeMessageHeader() throws IOException {
        incrementResourceCount();
        boolean ok = false;
        try {
            final WrappedOutputStream os = new WrappedOutputStream(channel.writeMessage());
            streams.add(os);
            try {
                writeInt(os, conversationID);
                os.write(CONVERSATION_MESSAGE);
                ok = true;
                return os;
            } finally {
                if (!ok) {
                    IoUtils.safeClose(os);
                    ok = true;
                }
            }
        } finally {
            if (!ok) {
                resourceFinished();
            }
        }
    }

    @Override
    public void handleError(IOException e) {
        closeAsync();
    }

    protected void handleRemoteClose() {
        prepareClose();
        closeAsync();
    }

    @Override
    protected void closeAction() throws IOException {
        if (prepareClose()) {
            try {
                final OutputStream os = channel.writeMessage();
                writeInt(os, conversationID);
                os.write(CONVERSATION_CLOSED);
                os.close();
            } catch (IOException e) {
                log.debugf(e, "failed to send closed message");
            }
        }
        final Set<Cancellable> cancellables;
        synchronized (streams) {
            cancellables = new HashSet<Cancellable>(streams);
        }
        for (final Cancellable cancellable : cancellables) {
            cancellable.cancel();
        }
        for (final PendingRequest request : pendingRequests.values()) {
            request.handleCancellation(this);
        }
    }

    private void resourceFinished(final Cancellable cancellable) {
        try {
            streams.remove(cancellable);
        } finally {
            resourceFinished();
        }
    }

    private class PendingRequest implements Conversation.MessageHandler {

        private boolean done;
        private final Conversation.MessageHandler messageHandler;
        private PendingRequest(Conversation.MessageHandler messageHandler) {
            this.messageHandler = messageHandler;
        }

        @Override
        public void handleMessage(Conversation conversation, InputStream message) throws IOException {
            try {
                messageHandler.handleMessage(conversation, message);
            } catch (IOException e) {
                messageHandler.handleError(conversation, e);
            } catch (Throwable t) {
                // Unhandled errors result in the termination of the conversation
                conversation.handleError(new IOException(t));
            } finally {
                done();
            }
        }

        @Override
        public void handleError(Conversation conversation, IOException e) {
            try {
                messageHandler.handleError(conversation, e);
            } catch (Throwable t) {
                conversation.handleError(new IOException(t));
            } finally {
                done();
            }
        }

        @Override
        public void handleCancellation(Conversation conversation) {
            try {
                messageHandler.handleCancellation(conversation);
            } catch (Throwable t) {
                conversation.handleError(new IOException(t));
            } finally {
                done();
            }
        }

        private synchronized void done() {
            if (done) {
                return;
            }
            done = true;
            resourceFinished();
        }

    }

    class WrappedInputStream extends FilterInputStream implements Cancellable {

        private boolean closed;
        WrappedInputStream(InputStream in) {
            super(in);
        }

        @Override
        public Cancellable cancel() {
            IoUtils.safeClose(this);
            return this;
        }

        @Override
        public synchronized void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            try {
                super.close();
            } finally {
                resourceFinished(this);
            }
        }
    }

    class WrappedOutputStream extends FilterOutputStream implements Cancellable {

        private boolean closed;
        private final Cancellable cancellable;
        WrappedOutputStream(MessageOutputStream out) {
            super(out);
            cancellable = out;
        }

        @Override
        public synchronized Cancellable cancel() {
            if (closed) {
                return this;
            }
            closed = true;
            try {
                cancellable.cancel();
                return this;
            } finally {
                resourceFinished(this);
            }
        }

        @Override
        public synchronized void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            try {
                super.close();
            } finally {
                resourceFinished(this);
            }
        }
    }

}