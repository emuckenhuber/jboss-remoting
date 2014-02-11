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

/**
 * @author Emanuel Muckenhuber
 */
class Protocol {

    //The highest-supported version of the protocol supported by this implementation.
    static final byte VERSION = 1;

    // Open a new conversation
    static final byte OPEN_CONVERSATION = 0x10;
    // conversation opened
    static final byte CONVERSATION_OPENED = 0x11;
    // conversation rejected
    static final byte CONVERSATION_REJECTED = 0x12;
    // a message in a conversation
    static final byte CONVERSATION_MESSAGE = 0x13;
    // a conversation got closed
    static final byte CONVERSATION_CLOSED = 0x14;
    // a protocol error
    static final byte PROTOCOL_ERROR = 0x15;

    // Conversation message types

    // A new request message
    static final byte NEW_REQUEST_MESSAGE = 0x20;
    // A response message
    static final byte RESPONSE_MESSAGE = 0x21;
    // The message body
    static final byte MESSAGE_BODY = 0x22;
    // The request id (used on the remote side to respond)
    static final byte REQUEST_ID = 0x23;
    // Message failure
    static final byte MESSAGE_FAILED = 0x30;

    // Capabilities

    static final byte CAP_VERSION = 0;   // sent by client & server - max version supported (must be first)

    static final byte CAP_SINGLE_CONVERSATION = 1; // only allow single conversation per channel

    static final byte USER_CAP = 0x79;   // user capabilities

}
