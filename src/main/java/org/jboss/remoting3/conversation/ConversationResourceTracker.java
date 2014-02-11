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
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.jboss.remoting3.HandleableCloseable;
import org.jboss.remoting3.NotOpenException;
import org.jboss.remoting3.spi.AbstractHandleableCloseable;

/**
 * @author Emanuel Muckenhuber
 */
class ConversationResourceTracker<T extends HandleableCloseable<T>> extends AbstractHandleableCloseable<T> {

    private volatile int state;
    private static final int CLOSED_FLAG = 1 << 31;
    private static final AtomicIntegerFieldUpdater<ConversationResourceTracker> stateUpdater = AtomicIntegerFieldUpdater.newUpdater(ConversationResourceTracker.class, "state");

    ConversationResourceTracker(Executor executor) {
        super(executor);
    }

    // Set the state to closing
    protected boolean prepareClose() {
        int oldState, newState;
        do {
            oldState = state;
            if ((oldState & CLOSED_FLAG) != 0) {
                return false;
            }
            newState = oldState | CLOSED_FLAG;
        } while (!stateUpdater.compareAndSet(this, oldState, newState));
        if (newState == CLOSED_FLAG) {
            closeComplete();
        }
        return true;
    }

    protected final void incrementResourceCount() throws IOException {
        if (!incrementResourceCountUnchecked()) {
            throw new NotOpenException();
        }
    }

    protected final boolean incrementResourceCountUnchecked() {
        int old;
        do {
            old = stateUpdater.get(this);
            if ((old & CLOSED_FLAG) != 0) {
                return false;
            }
        } while (!stateUpdater.compareAndSet(this, old, old + 1));
        return true;
    }

    protected void resourceFinished() {
        int res = stateUpdater.decrementAndGet(this);
        if (res == CLOSED_FLAG) {
            closeComplete();
        }
    }

}
