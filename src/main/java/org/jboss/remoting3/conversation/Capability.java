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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * A user defined protocol capability.
 *
 * @author Emanuel Muckenhuber
 */
public abstract class Capability<T> {

    protected final Byte id;
    protected final Class<T> type;
    protected Capability(final byte id, final Class<T> type) {
        this.id = Byte.valueOf(id);
        this.type = type;
    }

    /**
     * Get the capability id. This has to be unique per conversation.
     *
     * @return the capability id
     */
    public final Byte getCapabilityID() {
        return id;
    }

    /**
     * Get the protocol payload.
     *
     * @return the payload
     */
    public abstract byte[] getPayload(T value);

    /**
     * Create from payload.
     *
     * @param data  the payload
     * @param value the local value
     * @return the capability
     */
    public abstract T fromPayLoad(byte[] data, T value);

    protected T cast(Object o) {
        return type.cast(o);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Capability)) {
            return false;
        }

        final Capability other = (Capability) o;
        if (id != other.id) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    public static class BooleanCapability extends Capability<Boolean> {

        public BooleanCapability(byte id) {
            super(id, Boolean.class);
        }

        @Override
        public byte[] getPayload(Boolean value) {
            return value ? new byte[] { 1 } : new byte[] { 0 };
        }

        @Override
        public Boolean fromPayLoad(byte[] data, Boolean value) {
            if (value) {
                return Boolean.valueOf(data[0] == 1);
            } else {
                return Boolean.FALSE;
            }
        }

    }

    public static class MinIntegerCapability extends Capability<Integer> {

        public MinIntegerCapability(byte id) {
            super(id, Integer.class);
        }

        @Override
        public byte[] getPayload(Integer value) {
            return ByteBuffer.allocate(4).putInt(value).array();
        }

        @Override
        public Integer fromPayLoad(byte[] data, Integer value) {
            final Integer payload = ByteBuffer.wrap(data).getInt();
            return Math.min(value, payload);
        }
    }


    public static class StringCapability extends Capability<String> {

        public StringCapability(byte id, Class<String> type) {
            super(id, type);
        }

        @Override
        public byte[] getPayload(String value) {
            return new byte[0];
        }

        @Override
        public String fromPayLoad(byte[] data, String value) {
            final String ov = new String(data, StandardCharsets.UTF_8);
            if (value != null ? !value.equals(ov) : ov != null) {
                return ov;
            }
            return null;
        }
    }

}
