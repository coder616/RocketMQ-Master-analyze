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
package com.alibaba.rocketmq.remoting.netty;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.alibaba.rocketmq.remoting.InvokeCallback;
import com.alibaba.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;


/**
 * 异步请求应答封装
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-13
 */
public class ResponseFuture {
    // netty 通信数据协议对象
    private volatile RemotingCommand responseCommand;
    // 发送请求的状态，成功还是不成功
    private volatile boolean sendRequestOK = true;
    // 异常
    private volatile Throwable cause;
    // response 编号
    private final int opaque;
    // 超时时间
    private final long timeoutMillis;
    // 回调接口
    private final InvokeCallback invokeCallback;
    // 开始时间戳
    private final long beginTimestamp = System.currentTimeMillis();
    // 是一个倒计数的锁存器，当计数减至0时触发特定的事件
    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    // 保证信号量至多至少只被释放一次
    private final SemaphoreReleaseOnlyOnce once;

    // 保证回调的callback方法至多至少只被执行一次
    private final AtomicBoolean executeCallbackOnlyOnce = new AtomicBoolean(false);


    public ResponseFuture(int opaque, long timeoutMillis, InvokeCallback invokeCallback,
            SemaphoreReleaseOnlyOnce once) {
        this.opaque = opaque;
        this.timeoutMillis = timeoutMillis;
        this.invokeCallback = invokeCallback;
        this.once = once;
    }


    /**
     * 执行回调
     */
    public void executeInvokeCallback() {
        if (invokeCallback != null) {
            if (this.executeCallbackOnlyOnce.compareAndSet(false, true)) {
                invokeCallback.operationComplete(this);
            }
        }
    }


    // 释放信号量
    public void release() {
        if (this.once != null) {
            this.once.release();
        }
    }


    // 判断是否超时
    public boolean isTimeout() {
        long diff = System.currentTimeMillis() - this.beginTimestamp;
        return diff > this.timeoutMillis;
    }


    // 等待响应
    public RemotingCommand waitResponse(final long timeoutMillis) throws InterruptedException {
        // await()方法会阻塞程序继续执行
        // 在timeoutMillis（超时时间）时长内等待countDownLatch被减至0时再继续往下执行
        this.countDownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        return this.responseCommand;
    }


    public void putResponse(final RemotingCommand responseCommand) {
        // 设置response
        this.responseCommand = responseCommand;
        // 每调用一次countDown()方法，计数器减1
        this.countDownLatch.countDown();
    }


    public long getBeginTimestamp() {
        return beginTimestamp;
    }


    public boolean isSendRequestOK() {
        return sendRequestOK;
    }


    public void setSendRequestOK(boolean sendRequestOK) {
        this.sendRequestOK = sendRequestOK;
    }


    public long getTimeoutMillis() {
        return timeoutMillis;
    }


    public InvokeCallback getInvokeCallback() {
        return invokeCallback;
    }


    public Throwable getCause() {
        return cause;
    }


    public void setCause(Throwable cause) {
        this.cause = cause;
    }


    public RemotingCommand getResponseCommand() {
        return responseCommand;
    }


    public void setResponseCommand(RemotingCommand responseCommand) {
        this.responseCommand = responseCommand;
    }


    public int getOpaque() {
        return opaque;
    }


    @Override
    public String toString() {
        return "ResponseFuture [responseCommand=" + responseCommand + ", sendRequestOK=" + sendRequestOK
                + ", cause=" + cause + ", opaque=" + opaque + ", timeoutMillis=" + timeoutMillis
                + ", invokeCallback=" + invokeCallback + ", beginTimestamp=" + beginTimestamp
                + ", countDownLatch=" + countDownLatch + "]";
    }
}
