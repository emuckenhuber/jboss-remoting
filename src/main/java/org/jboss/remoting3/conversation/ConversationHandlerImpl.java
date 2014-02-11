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
import static org.jboss.remoting3.conversation.ProtocolUtils.readProtocolError;
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
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;

import org.jboss.remoting3.Attachments;
import org.jboss.remoting3.Channel;
import org.jboss.remoting3.CloseHandler;
import org.jboss.remoting3.MessageInputStream;
import org.jboss.remoting3.MessageOutputStream;
import org.jboss.remoting3._private.IntIndexHashMap;
import org.jboss.remoting3._private.IntIndexer;
import org.xnio.Cancellable;
import org.xnio.IoUtils;
import org.xnio.Option;

/**
 * @author Emanuel Muckenhuber
 */
class ConversationHandlerImpl extends ConversationResourceTracker<Conversation> implements Conversation {

    static final IntIndexer<ConversationHandlerImpl> CONVERSATION_INT_INDEXER = new IntIndexer<ConversationHandlerImpl>() {
        @Override
        public int getKey(ConversationHandlerImpl argument) {
            return argument.localConversationID;
        }
    };

    private static final ConversationLogger log = ConversationLogger.ROOT_LOGGER;
    private static final IntIndexer<PendingRequest> REQUEST_INT_INDEXER = new IntIndexer<PendingRequest>() {
        @Override
        public int getKey(PendingRequest argument) {
            return argument.requestID;
        }
    };

    private final int conversationID; // The conversation ID used to send messages to the remote side
    private final ChannelWrapper channel;
    private final Capabilities capabilities;
    private final int localConversationID; // The conversation ID we locally receive messages
    private final Attachments attachments = new Attachments();
    private final ConversationMessageReceiver messageReceiver;
    private final Set<Cancellable> streams = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<Cancellable, Boolean>()));
    private final IntIndexHashMap<PendingRequest> pendingRequests = new IntIndexHashMap<PendingRequest>(REQUEST_INT_INDEXER);
    private final CloseHandler<Channel> closeHandler = new CloseHandler<Channel>() {
        @Override
        public void handleClose(Channel closed, IOException exception) {
            handleRemoteClose();
        }
    };

    public ConversationHandlerImpl(final int localConversationID, final int conversationID, final ConversationMessageReceiver messageReceiver, final ChannelWrapper channel,
                                   final Capabilities capabilities, final Executor executor) {
        super(executor);
        this.channel = channel;
        this.capabilities = capabilities;
        this.messageReceiver = messageReceiver;
        this.localConversationID = localConversationID;
        this.conversationID = conversationID;
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
        if (option == ConversationOptions.USER_CAPABILITIES) {
            return option.cast(capabilities);
        }
        return channel.getOption(option);
    }

    @Override
    public <T> T setOption(Option<T> option, T value) throws IllegalArgumentException, IOException {
        return channel.setOption(option, value);
    }

    @Override
    public boolean hasCapability(Capability<?> capability) {
        return capabilities.contains(capability);
    }

    @Override
    public <T> boolean supportsCapability(Capability<T> capability, T value) {
        return capabilities.matches(capability, value);
    }

    @Override
    public <T> T getCapability(Capability<T> capability) {
        return capabilities.get(capability);
    }

    @Override
    public OutputStream writeMessage() throws IOException {
        boolean ok = false;
        final OutputStream os = writeMessageHeader(null);
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
    public OutputStream writeMessage(MessageHandler messageHandler) throws IOException {
        final int requestID = newPendingRequest(messageHandler);

        boolean ok = false;
        final OutputStream os = writeMessageHeader(null);
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
     * @param message        the message input stream
     * @throws IOException
     */
    protected void handleMessage(final int conversationID, final MessageInputStream message) throws IOException {
        assert conversationID == localConversationID;
        final int type = message.read();
        if (type == NEW_REQUEST_MESSAGE) {
            final int requestType = message.read();
            final Conversation conversation;
            if (requestType == REQUEST_ID) { // Expects a response
                final int responseID = readInt(message);
                expectHeader(message, MESSAGE_BODY);
                conversation = new ConversationResponseImpl(this, responseID);
            } else if (requestType == MESSAGE_BODY) { // One way message
                conversation = new OneWayConversationResponse(this);
            } else {
                IoUtils.safeClose(message);
                sendProtocolError(conversationID, channel, "unknown request type");
                return;
            }
            if (messageReceiver == null) {
                try {
                    conversation.handleError(new IOException("cannot accept initial requests"));
                } finally {
                    IoUtils.safeClose(message);
                }
            }
            if (incrementResourceCountUnchecked()) {
                final TrackingInputStream is = new TrackingInputStream(message);
                streams.add(is);
                boolean ok = false;
                try {
                    messageReceiver.handleMessage(conversation, is);
                    ok = true;
                } catch (IOException e) {
                    conversation.handleError(e);
                } catch (Exception e) {
                    conversation.handleError(new IOException(e));
                } finally {
                    if (!ok) {
                        IoUtils.safeClose(is);
                    }
                }
            } else {
                log.debugf("conversation closed. discarding message.");
                IoUtils.safeClose(message);
            }
        } else if (type == RESPONSE_MESSAGE) {
            final int requestID = readInt(message);
            final PendingRequest request = pendingRequests.removeKey(requestID);
            final int responseType = message.read();
            if (responseType == MESSAGE_FAILED) {
                try {
                    if (request == null) {
                        log.errorf("no message handler for request %s", requestID);
                    } else {
                        final String error = readProtocolError(message);
                        request.handleError(this, new IOException(error));
                    }
                    return;
                } finally {
                    IoUtils.safeClose(message);
                }
            } else {
                final Conversation conversation;
                if (responseType == REQUEST_ID) { // Expects a response
                    final int responseID = readInt(message);
                    expectHeader(message, MESSAGE_BODY);
                    conversation = new ConversationResponseImpl(this, responseID);
                } else if (responseType == MESSAGE_BODY) { // One way
                    conversation = new OneWayConversationResponse(this);
                } else {
                    IoUtils.safeClose(message);
                    sendProtocolError(conversationID, channel, "unknown request type");
                    return;
                }
                if (request == null) {
                    try {
                        conversation.handleError(new IOException("no message handler for request " + requestID));
                    } finally {
                        IoUtils.safeClose(message);
                    }
                    return;
                }
                if (incrementResourceCountUnchecked()) {
                    final TrackingInputStream is = new TrackingInputStream(message);
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
                } else {
                    log.debugf("conversation closed. discarding message.");
                    IoUtils.safeClose(message);
                }
            }
        }
    }

    // Create a new pending request
    protected int newPendingRequest(final Conversation.MessageHandler messageHandler) throws IOException {
        incrementResourceCount();
        PendingRequest request;
        for (;;) {
            final int requestID = ThreadLocalRandom.current().nextInt();
            request = new PendingRequest(requestID, messageHandler);
            if (pendingRequests.putIfAbsent(request) == null) {
                break;
            }
        }
        return request.requestID;
    }

    // Handle immediate local failures
    protected void handleFailure(final int requestID, Conversation conversation, final IOException e) {
        final PendingRequest request = pendingRequests.removeKey(requestID);
        if (request != null) {
            request.handleError(conversation, e);
        }
    }

    // Write the message header, increase the resource count and wrap the output stream
    protected OutputStream writeMessageHeader(Thread thread) throws IOException {
        incrementResourceCount();
        boolean ok = false;
        try {
            final TrackingOutputStream os = new TrackingOutputStream(channel.writeMessageChecked(thread));
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
        ConversationLogger.ROOT_LOGGER.debugf(e, "terminating conversation %s", conversationID);
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
            } finally {
                channel.release();
            }
        }
        final Set<Cancellable> cancellables;
        synchronized (streams) {
            cancellables = new HashSet<Cancellable>(streams);
        }
        for (final Cancellable cancellable : cancellables) {
            cancellable.cancel();
        }
        final Iterator<PendingRequest> i = pendingRequests.iterator();
        while (i.hasNext()) {
            i.next().handleCancellation(this);
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
        private final int requestID;
        private final Conversation.MessageHandler messageHandler;
        private PendingRequest(int requestID, Conversation.MessageHandler messageHandler) {
            this.messageHandler = messageHandler;
            this.requestID = requestID;
        }

        @Override
        public void handleMessage(Conversation conversation, InputStream message) throws IOException {
            boolean ok = false;
            try {
                messageHandler.handleMessage(conversation, message);
                ok = true;
            } catch (IOException e) {
                messageHandler.handleError(conversation, e);
            } catch (Throwable t) {
                // Unhandled errors result in the termination of the conversation
                conversation.handleError(new IOException(t));
            } finally {
                done();
                if (!ok) {
                    IoUtils.safeClose(message);
                }
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

    class TrackingInputStream extends FilterInputStream implements Cancellable {

        private boolean closed;
        TrackingInputStream(InputStream in) {
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

    class TrackingOutputStream extends FilterOutputStream implements Cancellable {

        private boolean closed;
        private final MessageOutputStream out;
        TrackingOutputStream(MessageOutputStream out) {
            super(out);
            this.out = out;
        }

        @Override
        public synchronized Cancellable cancel() {
            if (closed) {
                return this;
            }
            closed = true;
            try {
                out.cancel();
                return this;
            } finally {
                try {
                    channel.release();
                } finally {
                    resourceFinished(this);
                }
            }
        }

        @Override
        public synchronized void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            try {
                out.close();
            } finally {
                try {
                    channel.release();
                } finally {
                    resourceFinished(this);
                }
            }
        }
    }

}
