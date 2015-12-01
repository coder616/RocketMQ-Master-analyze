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
package com.alibaba.rocketmq.store;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.ServiceThread;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.constant.LoggerName;


/**
 * Create MapedFile in advance 映射文件分配服务
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public class AllocateMapedFileService extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);
    private static int WaitTimeOut = 1000 * 5;
    // 请求列表
    private ConcurrentHashMap<String, AllocateRequest> requestTable =
            new ConcurrentHashMap<String, AllocateRequest>();
    // 优先级阻塞队列
    private PriorityBlockingQueue<AllocateRequest> requestQueue =
            new PriorityBlockingQueue<AllocateRequest>();
    private volatile boolean hasException = false;


    /**
     * 创建映射文件队列
     *
     * @param nextFilePath
     *            下一个文件的路径
     * @param nextNextFilePath
     *            下下个文件路径
     * @param fileSize
     *            文件大小
     * @return
     */
    public MapedFile putRequestAndReturnMapedFile(String nextFilePath, String nextNextFilePath,
            int fileSize) {
        AllocateRequest nextReq = new AllocateRequest(nextFilePath, fileSize);
        AllocateRequest nextNextReq = new AllocateRequest(nextNextFilePath, fileSize);
        boolean nextPutOK = (this.requestTable.putIfAbsent(nextFilePath, nextReq) == null);
        boolean nextNextPutOK = (this.requestTable.putIfAbsent(nextNextFilePath, nextNextReq) == null);

        if (nextPutOK) {
            // offer 将指定的元素插入到优先级队列中。
            boolean offerOK = this.requestQueue.offer(nextReq);
            if (!offerOK) {
                log.warn("add a request to preallocate queue failed");
            }
        }

        if (nextNextPutOK) {
            boolean offerOK = this.requestQueue.offer(nextNextReq);
            if (!offerOK) {
                log.warn("add a request to preallocate queue failed");
            }
        }

        if (hasException) {
            log.warn(this.getServiceName() + " service has exception. so return null");
            return null;
        }

        AllocateRequest result = this.requestTable.get(nextFilePath);
        try {
            if (result != null) {
                // 等待WaitTimeOut时长之后，如果mapedfile没有创建完成，删除请求
                boolean waitOK = result.getCountDownLatch().await(WaitTimeOut, TimeUnit.MILLISECONDS);
                if (!waitOK) {
                    log.warn("create mmap timeout " + result.getFilePath() + " " + result.getFileSize());
                }
                this.requestTable.remove(nextFilePath);
                return result.getMapedFile();

            }
            else {
                log.error("find preallocate mmap failed, this never happen");
            }
        }
        catch (InterruptedException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
        }

        return null;
    }


    @Override
    public String getServiceName() {
        return AllocateMapedFileService.class.getSimpleName();
    }


    public void shutdown() {
        this.stoped = true;
        this.thread.interrupt();

        try {
            this.thread.join(this.getJointime());
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (AllocateRequest req : this.requestTable.values()) {
            if (req.mapedFile != null) {
                log.info("delete pre allocated maped file, {}", req.mapedFile.getFileName());
                req.mapedFile.destroy(1000);
            }
        }
    }


    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStoped() && this.mmapOperation())
            ;

        log.info(this.getServiceName() + " service end");
    }


    /**
     * 创建具体的映射文件
     */
    private boolean mmapOperation() {
        AllocateRequest req = null;
        try {
            // take 检索并移除此队列的头部，如果此队列不存在任何元素，则一直等待。
            req = this.requestQueue.take();
            if (null == this.requestTable.get(req.getFilePath())) {
                log.warn("this mmap request expired, maybe cause timeout " + req.getFilePath() + " "
                        + req.getFileSize());
                return true;
            }

            if (req.getMapedFile() == null) {
                long beginTime = System.currentTimeMillis();
                MapedFile mapedFile = new MapedFile(req.getFilePath(), req.getFileSize());
                long eclipseTime = UtilAll.computeEclipseTimeMilliseconds(beginTime);
                if (eclipseTime > 10) {
                    int queueSize = this.requestQueue.size();
                    log.warn("create mapedFile spent time(ms) " + eclipseTime + " queue size " + queueSize
                            + " " + req.getFilePath() + " " + req.getFileSize());
                }

                req.setMapedFile(mapedFile);
                this.hasException = false;
            }
        }
        catch (InterruptedException e) {
            log.warn(this.getServiceName() + " service has exception, maybe by shutdown");
            this.hasException = true;
            return false;
        }
        catch (IOException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
            this.hasException = true;
        }
        finally {
            if (req != null)
                // 创建完成计数器减一
                req.getCountDownLatch().countDown();
        }
        return true;
    }

    // 分配请求类
    class AllocateRequest implements Comparable<AllocateRequest> {
        // 文件的全路径名
        private String filePath;
        // 文件大小
        private int fileSize;
        // 倒序计数器
        private CountDownLatch countDownLatch = new CountDownLatch(1);
        // 映射文件
        private volatile MapedFile mapedFile = null;


        public AllocateRequest(String filePath, int fileSize) {
            this.filePath = filePath;
            this.fileSize = fileSize;
        }


        public String getFilePath() {
            return filePath;
        }


        public void setFilePath(String filePath) {
            this.filePath = filePath;
        }


        public int getFileSize() {
            return fileSize;
        }


        public void setFileSize(int fileSize) {
            this.fileSize = fileSize;
        }


        public CountDownLatch getCountDownLatch() {
            return countDownLatch;
        }


        public void setCountDownLatch(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }


        public MapedFile getMapedFile() {
            return mapedFile;
        }


        public void setMapedFile(MapedFile mapedFile) {
            this.mapedFile = mapedFile;
        }


        public int compareTo(AllocateRequest other) {
            return this.fileSize < other.fileSize ? 1 : this.fileSize > other.fileSize ? -1 : 0;
        }
    }
}
