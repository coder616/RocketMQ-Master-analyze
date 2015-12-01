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
package com.alibaba.rocketmq.remoting.common;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * 使用布尔原子变量，信号量保证只释放一次
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-13
 */
public class SemaphoreReleaseOnlyOnce {
    // 原子Boolean
    private final AtomicBoolean released = new AtomicBoolean(false);
    // 计数信号量
    private final Semaphore semaphore;


    public SemaphoreReleaseOnlyOnce(Semaphore semaphore) {
        this.semaphore = semaphore;
    }


    public void release() {
        if (this.semaphore != null) {
            /**
             * compareAndSet(boolean expect, boolean update)这个方法主要两个作用 1.
             * 比较AtomicBoolean和expect的值，如果一致，执行方法内的语句。其实就是一个if语句
             * 2.把AtomicBoolean的值设成update
             * 比较最要的是这两件事是一气呵成的，这连个动作之间不会被打断，任何内部或者外部的语句都不可能在两个动作之间运行。
             */
            if (this.released.compareAndSet(false, true)) {
                this.semaphore.release();
            }
        }
    }


    public Semaphore getSemaphore() {
        return semaphore;
    }
}
