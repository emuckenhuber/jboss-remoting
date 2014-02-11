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

import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Capabilities are negotiated when establishing a new conversation and represent user defined properties which can
 * also be used as a backwards compatibility mechanism of the protocol. When opening a conversation the client can
 * request capabilities, which are then evaluated on the remote side. The final set of capabilities are sent back
 * to the client when the conversation is established. For example the absence of a capability on the server side
 * still allows the conversation to be opened, however it would mean that a specific protocol features cannot be used.
 *
 * @author Emanuel Muckenhuber
 */
public final class Capabilities {

    protected static final Capabilities EMPTY = newBuilder().build();

    private final Map<Byte, Value<?>> capabilities;
    protected Capabilities(final Map<Byte, Value<?>> capabilities) {
        this.capabilities = Collections.unmodifiableMap(capabilities);
    }

    public boolean contains(final byte capabilityId) {
        return capabilities.containsKey(capabilityId);
    }

    public boolean contains(final Capability<?> capability) {
        return capabilities.containsKey(capability.getCapabilityID());
    }

    public <T> boolean matches(final Capability<T> capability, T reference) {
        final T value = get(capability);
        return value == null ? reference == null : value.equals(reference);
    }

    public Capability<?> getCapability(final byte id) {
        final Byte key = Byte.valueOf(id);
        final Value<?> value = capabilities.get(key);
        return value != null ? value.capability : null;
    }

    public <T> T get(final Capability<T> capability) {
        final Value<?> value = capabilities.get(capability.getCapabilityID());
        return value != null ? capability.cast(value.value) : null;
    }

    /**
     * Get all capabilities.
     *
     * @return the capabilities.
     */
    Collection<Byte> keySet() {
        return capabilities.keySet();
    }

    /**
     * Create a new capabilities builder.
     *
     * @return the builder
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {

        private Map<Byte, Value<?>> capabilities = Collections.emptyMap();
        protected Builder() {
            //
        }

        /**
         * Put a capability.
         *
         * @param capability the capability
         * @return the previous capability associated with the capability id, <code>null</code> if there was no mapping
         */
        public <T> T put(Capability<T> capability, T value) {
            final Value<T> v = new Value<T>(capability, value);
            final int size = capabilities.size();
            if (size == 0) {
                capabilities = Collections.<Byte, Value<?>>singletonMap(capability.getCapabilityID(), v);
                return null;
            } else if (size == 1) {
                capabilities = new IdentityHashMap<Byte, Value<?>>();
            }
            final Value<?> e = capabilities.put(capability.getCapabilityID(), v);
            return e != null ? capability.cast(e.value) : null;
        }

        /**
         * Build the final capabilities.
         *
         * @return the capabilities
         */
        public Capabilities build() {
            return new Capabilities(capabilities);
        }

    }

    static class Value<T> {

        private final Capability<T> capability;
        private final T value;

        Value(Capability<T> capability, T value) {
            this.capability = capability;
            this.value = value;
        }

    }

}
