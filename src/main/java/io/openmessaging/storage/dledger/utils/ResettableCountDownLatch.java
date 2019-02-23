/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger.utils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * 可重置的 CountDownLatch
 * Add reset feature for @see java.util.concurrent.CountDownLatch
 */
public class ResettableCountDownLatch {
    private final Sync sync;

    /**
     * Constructs a {@code CountDownLatch2} initialized with the given count.
     *
     * @param count the number of times {@link #countDown} must be invoked before threads can pass through {@link
     *              #await}
     * @throws IllegalArgumentException if {@code count} is negative
     */
    public ResettableCountDownLatch(int count) {
        if (count < 0) {
            throw new IllegalArgumentException("count < 0");
        }
        this.sync = new Sync(count);
    }

    /**
     * Causes the current thread to wait until the latch has counted down to
     * zero, unless the thread is {@linkplain Thread#interrupt interrupted}.
     *
     * <p>If the current count is zero then this method returns immediately.
     *
     * <p>If the current count is greater than zero then the current
     * thread becomes disabled for thread scheduling purposes and lies
     * dormant until one of two things happen:
     * <ul>
     * <li>The count reaches zero due to invocations of the
     * {@link #countDown} method; or
     * <li>Some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread.
     * </ul>
     *
     * <p>If the current thread:
     * <ul>
     * <li>has its interrupted status set on entry to this method; or
     * <li>is {@linkplain Thread#interrupt interrupted} while waiting,
     * </ul>
     * then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.
     *
     * @throws InterruptedException if the current thread is interrupted while waiting
     *                              <p>
     *                              <p>
     *                              这里的 await 的实现是通过获取锁的方式，当state减到0的时候，就可以获取锁，然后这里相当于就等待结束。
     */
    public void await() throws InterruptedException {
        // 共享式获取同步状态，响应中断
        sync.acquireSharedInterruptibly(1);
    }

    /**
     * Causes the current thread to wait until the latch has counted down to
     * zero, unless the thread is {@linkplain Thread#interrupt interrupted},
     * or the specified waiting time elapses.
     *
     * <p>If the current count is zero then this method returns immediately
     * with the value {@code true}.
     *
     * <p>If the current count is greater than zero then the current
     * thread becomes disabled for thread scheduling purposes and lies
     * dormant until one of three things happen:
     * <ul>
     * <li>The count reaches zero due to invocations of the
     * {@link #countDown} method; or
     * <li>Some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread; or
     * <li>The specified waiting time elapses.
     * </ul>
     *
     * <p>If the count reaches zero then the method returns with the
     * value {@code true}.
     *
     * <p>If the current thread:
     * <ul>
     * <li>has its interrupted status set on entry to this method; or
     * <li>is {@linkplain Thread#interrupt interrupted} while waiting,
     * </ul>
     * then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.
     *
     * <p>If the specified waiting time elapses then the value {@code false}
     * is returned.  If the time is less than or equal to zero, the method
     * will not wait at all.
     *
     * @param timeout the maximum time to wait
     * @param unit    the time unit of the {@code timeout} argument
     * @return {@code true} if the count reached zero and {@code false} if the waiting time elapsed before the count
     * reached zero
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public boolean await(long timeout, TimeUnit unit)
            throws InterruptedException {
        // 共享式获取同步状态，增加超时限制
        return sync.tryAcquireSharedNanos(1, unit.toNanos(timeout));
    }

    /**
     * Decrements the count of the latch, releasing all waiting threads if
     * the count reaches zero.
     *
     * <p>If the current count is greater than zero then it is decremented.
     * If the new count is zero then all waiting threads are re-enabled for
     * thread scheduling purposes.
     *
     * <p>If the current count equals zero then nothing happens.
     */
    public void countDown() {
        sync.releaseShared(1);
    }

    /**
     * Returns the current count.
     *
     * <p>This method is typically used for debugging and testing purposes.
     *
     * @return the current count
     */
    public long getCount() {
        return sync.getCount();
    }

    public void reset() {
        sync.reset();
    }

    /**
     * Returns a string identifying this latch, as well as its state.
     * The state, in brackets, includes the String {@code "Count ="}
     * followed by the current count.
     *
     * @return a string identifying this latch, as well as its state
     */
    @Override
    public String toString() {
        return super.toString() + "[Count = " + sync.getCount() + "]";
    }

    /**
     * Synchronization control For CountDownLatch2.
     * Uses AQS(AbstractQueuedSynchronizer, 队列同步器) state to represent count.
     * AQS使用一个int类型的成员变量state来表示同步状态，当state>0时表示已经获取了锁，当state = 0时表示释放了锁。
     * 它提供了三个方法（getState()、setState(int newState)、compareAndSetState(int expect,int update)）
     * 来对同步状态state进行操作，当然AQS可以确保对state的操作是安全的。
     * 参考：http://cmsblogs.com/?p=2174
     */
    private static final class Sync extends AbstractQueuedSynchronizer {
        private static final long serialVersionUID = 4982264981922014374L;

        private final int startCount;

        /**
         * 构造函数new的时候重置一次，后续调用reset的时候会再重置一次。
         */
        Sync(int count) {

            /**
             * 自定义一个startCount用来记录起始count, 方便后面reset
             * */
            this.startCount = count;

            /**
             * setState method:
             * static final class Node {
             *     private volatile int state;
             * }
             * protected final void setState(int newState) {
             *     state = newState;
             * }
             * */
            setState(count);
        }

        int getCount() {
            return getState();
        }

        /**
         * 共享式获取同步状态，返回值大于等于0则表示获取成功
         *
         * @param acquires
         * @return
         */
        @Override
        protected int tryAcquireShared(int acquires) {
            // state == 0 锁已释放
            return (getState() == 0) ? 1 : -1;
        }

        /**
         * 共享式释放同步状态
         *
         * @param releases
         * @return
         */
        @Override
        protected boolean tryReleaseShared(int releases) {
            // Decrement count; signal when transition to zero
            for (; ; ) {
                int c = getState();
                if (c == 0) {
                    return false;
                }
                int nextc = c - 1;
                if (compareAndSetState(c, nextc)) {
                    return nextc == 0;
                }
            }
        }

        protected void reset() {
            setState(startCount);
        }
    }
}
