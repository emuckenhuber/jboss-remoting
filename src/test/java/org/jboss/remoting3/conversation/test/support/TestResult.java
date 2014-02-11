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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.xnio.IoFuture;

/**
 * @author Emanuel Muckenhuber
 */
class TestResult<T> implements Future<T> {

    private final IoFuture<T> result;
    public TestResult(IoFuture<T> result) {
        this.result = result;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return result.cancel().await() == IoFuture.Status.CANCELLED;
    }

    @Override
    public boolean isCancelled() {
        return result.getStatus() == IoFuture.Status.CANCELLED;
    }

    @Override
    public boolean isDone() {
        return result.getStatus() == IoFuture.Status.DONE;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        try {
            return result.get();
        } catch (IOException e) {
            throw new ExecutionException(e);
        }
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        final IoFuture.Status status = result.await(timeout, unit);
        if (status == IoFuture.Status.WAITING) {
            throw new TimeoutException();
        } else if (status == IoFuture.Status.CANCELLED) {
            throw new InterruptedException();
        }
        return get();
    }
}
