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

import static org.jboss.remoting3.conversation.Protocol.PROTOCOL_ERROR;
import static org.jboss.remoting3.conversation.Protocol.USER_CAP;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import org.jboss.remoting3.Channel;
import org.jboss.remoting3.MessageInputStream;
import org.xnio.IoUtils;

final class ProtocolUtils {

    private ProtocolUtils() {
        //
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

    static void expectHeader(final InputStream is, final byte expected) throws IOException {
        if (is.read() != expected) {
            throw new IOException("unexpected header");
        }
    }

    static void sendProtocolError(final int conversationID, final ChannelWrapper channel, String error) {
        try {
            final OutputStream os = channel.writeMessage();
            try {
                sendProtocolError(conversationID, os, error);
            } finally {
                channel.release();
            }
        } catch (IOException f) {
            ConversationLogger.ROOT_LOGGER.debugf(f, "failed to send error response");
        }
    }

    static void sendProtocolError(final int conversationID, final Channel channel, String error) {
        try {
            final OutputStream os = channel.writeMessage();
            sendProtocolError(conversationID, os, error);
        } catch (IOException f) {
            ConversationLogger.ROOT_LOGGER.debugf(f, "failed to send error response");
        }
    }

    static void sendProtocolError(final int conversationID, final OutputStream os, final String error) throws IOException {
        try {
            writeInt(os, conversationID);
            os.write(PROTOCOL_ERROR);
            writeErrorMessage(error, os);
            os.close();
        } finally {
            IoUtils.safeClose(os);
        }
    }

    static void writeErrorMessage(final OutputStream os, final Exception e) throws IOException {
        final String error = e.getMessage();
        if (error == null) {
            writeErrorMessage(e.getClass().getName(), os);
        } else {
            writeErrorMessage(error, os);
        }
    }

    static void writeErrorMessage(final String error, final OutputStream os) throws IOException {
        final byte[] data = error.getBytes(StandardCharsets.UTF_8);
        writeInt(os, data.length);
        os.write(data);
    }

    static String readProtocolError(final InputStream is) throws IOException {
        final int length = readInt(is);
        final byte[] data = new byte[length];
        is.read(data);
        return new String(data, StandardCharsets.UTF_8);
    }

    static void processUserCapabilities(final Capabilities.Builder builder, final Capabilities local, final MessageInputStream is) throws IOException {
        for (;;) {
            final int type = is.read();
            if (type == -1) {
                return;
            }
            final int len = readInt(is);
            final Capability<?> capability = local.getCapability((byte) type);
            if (capability == null) {
                is.skip(len);
            } else {
                final byte[] data = new byte[len];
                final int c = is.read(data);
                processPayload(capability, data, local, builder);
                if (c == -1) {
                    return;
                }
            }
        }
    }

    static void writeUserCapabilities(final Capabilities capabilities, final OutputStream os) throws IOException {
        os.write(USER_CAP);
        for (final byte id : capabilities.keySet()) {
            final Capability<?> capability = capabilities.getCapability(id);
            if (capability == null) {
                continue;
            }
            final byte[] data = getPayload(capability, capabilities);
            os.write(id);
            writeInt(os, data.length);
            os.write(data);
        }
    }

    static <T> void processPayload(final Capability<T> capability, final byte[] data, final Capabilities local, final Capabilities.Builder builder) {
        final T existing = local.get(capability);
        final T value = capability.fromPayLoad(data, existing);
        if (value != null) {
            builder.put(capability, value);
        }
    }

    static <T> byte[] getPayload(final Capability<T> capability, final Capabilities local) {
        final T value = local.get(capability);
        return capability.getPayload(value);
    }

}
