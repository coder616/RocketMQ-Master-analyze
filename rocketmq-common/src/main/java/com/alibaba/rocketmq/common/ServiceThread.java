/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.constant.LoggerName;


/**
 * 后台服务线程基类
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public abstract class ServiceThread implements Runnable {
    private static final Logger stlog = LoggerFactory.getLogger(LoggerName.CommonLoggerName);
    // 执行线程
    protected final Thread thread;
    // 线程回收时间，默认90S
    private static final long JoinTime = 90 * 1000;
    // 锁对否被释放
    protected volatile boolean hasNotified = false;
    // 线程是否已经停止
    protected volatile boolean stoped = false;


    public ServiceThread() {
        this.thread = new Thread(this, this.getServiceName());
    }


    public abstract String getServiceName();


    public void start() {
        this.thread.start();
    }


    public void shutdown() {
        this.shutdown(false);
    }


    public void stop() {
        this.stop(false);
    }


    public void makeStop() {
        this.stoped = true;
        stlog.info("makestop thread " + this.getServiceName());
    }

    /**
     * 中断线程
     * @param interrupt
     */
    public void stop(final boolean interrupt) {
        this.stoped = true;
        stlog.info("stop thread " + this.getServiceName() + " interrupt " + interrupt);
        //判断锁是否被释放，如果没有则释放锁
        synchronized (this) {
            if (!this.hasNotified) {
                this.hasNotified = true;
                this.notify();
            }
        }

        if (interrupt) {
            this.thread.interrupt();
        }
    }


    /**
     * 结束线程
     * 
     * @param interrupt
     *            是否中断
     */
    public void shutdown(final boolean interrupt) {
        this.stoped = true;
        stlog.info("shutdown thread " + this.getServiceName() + " interrupt " + interrupt);
        //判断锁是否被释放，如果没有则释放锁
        synchronized (this) {
            if (!this.hasNotified) {
                this.hasNotified = true;
                this.notify();
            }
        }
        //判断是否需要中断线程，如果是则中断，如果不是则等待90s 等待线程完成
        try {
            if (interrupt) {
                this.thread.interrupt();
            }

            long beginTime = System.currentTimeMillis();
            if (!this.thread.isDaemon()) {
                this.thread.join(this.getJointime());
            }
            long eclipseTime = System.currentTimeMillis() - beginTime;
            stlog.info("join thread " + this.getServiceName() + " eclipse time(ms) " + eclipseTime + " "
                    + this.getJointime());
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public void wakeup() {
        synchronized (this) {
            if (!this.hasNotified) {
                this.hasNotified = true;
                this.notify();
            }
        }
    }

    //等待线程获取锁在给定的时长
    protected void waitForRunning(long interval) {
        synchronized (this) {
            if (this.hasNotified) {
                this.hasNotified = false;
                this.onWaitEnd();
                return;
            }

            try {
                this.wait(interval);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            finally {
                this.hasNotified = false;
                this.onWaitEnd();
            }
        }
    }


    protected void onWaitEnd() {
    }


    public boolean isStoped() {
        return stoped;
    }


    public long getJointime() {
        return JoinTime;
    }
}
