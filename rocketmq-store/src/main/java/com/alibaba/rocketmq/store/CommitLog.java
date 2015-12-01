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

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.ServiceThread;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.message.MessageAccessor;
import com.alibaba.rocketmq.common.message.MessageConst;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.sysflag.MessageSysFlag;
import com.alibaba.rocketmq.store.config.BrokerRole;
import com.alibaba.rocketmq.store.config.FlushDiskType;
import com.alibaba.rocketmq.store.ha.HAService;
import com.alibaba.rocketmq.store.schedule.ScheduleMessageService;


/**
 * Store all metadata downtime for recovery, data protection reliability
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public class CommitLog {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);
    // Message's MAGIC CODE daa320a7
    public final static int MessageMagicCode = 0xAABBCCDD ^ 1880681586 + 8;
    // 消息结束标志
    private final static int BlankMagicCode = 0xBBCCDDEE ^ 1880681586 + 8;
    // 存储文件队列
    private final MapedFileQueue mapedFileQueue;
    private final DefaultMessageStore defaultMessageStore;
    // 刷盘服务
    private final FlushCommitLogService flushCommitLogService;
    // 存入消息后的回调
    private final AppendMessageCallback appendMessageCallback;
    //
    private HashMap<String/* topic-queueid */, Long/* offset */> topicQueueTable =
            new HashMap<String, Long>(1024);


    public CommitLog(final DefaultMessageStore defaultMessageStore) {
        this.mapedFileQueue =
                new MapedFileQueue(defaultMessageStore.getMessageStoreConfig().getStorePathCommitLog(),
                    defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog(),
                    defaultMessageStore.getAllocateMapedFileService());
        this.defaultMessageStore = defaultMessageStore;
        if (FlushDiskType.SYNC_FLUSH == defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
            // 同步刷盘
            this.flushCommitLogService = new GroupCommitService();
        }
        else {
            // 异步刷盘
            this.flushCommitLogService = new FlushRealTimeService();
        }

        this.appendMessageCallback = new DefaultAppendMessageCallback(
            defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());
    }


    // 加载文件
    public boolean load() {
        boolean result = this.mapedFileQueue.load();
        log.info("load commit log " + (result ? "OK" : "Failed"));
        return result;
    }


    // 启动
    public void start() {
        this.flushCommitLogService.start();// 启动刷盘服务线程
    }


    // 关闭
    public void shutdown() {
        this.flushCommitLogService.shutdown();// 停止刷盘服务线程
    }


    // 获取最小偏移量
    public long getMinOffset() {
        MapedFile mapedFile = this.mapedFileQueue.getFirstMapedFileOnLock();
        if (mapedFile != null) {
            if (mapedFile.isAvailable()) {
                return mapedFile.getFileFromOffset();
            }
            else {
                return this.rollNextFile(mapedFile.getFileFromOffset());
            }
        }

        return -1;
    }


    public long rollNextFile(final long offset) {
        int mapedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog();
        return (offset + mapedFileSize - offset % mapedFileSize);
    }


    // 获取最大偏移量
    public long getMaxOffset() {
        return this.mapedFileQueue.getMaxOffset();
    }


    // 删除过期文件
    public int deleteExpiredFile(final long expiredTime, final int deleteFilesInterval,
            final long intervalForcibly, final boolean cleanImmediately) {
        return this.mapedFileQueue.deleteExpiredFileByTime(expiredTime, deleteFilesInterval, intervalForcibly,
            cleanImmediately);
    }


    /**
     * Read CommitLog data, use data replication
     */
    public SelectMapedBufferResult getData(final long offset) {
        return this.getData(offset, (0 == offset ? true : false));
    }


    public SelectMapedBufferResult getData(final long offset, final boolean returnFirstOnNotFound) {
        int mapedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog();
        MapedFile mapedFile = this.mapedFileQueue.findMapedFileByOffset(offset, returnFirstOnNotFound);
        if (mapedFile != null) {
            int pos = (int) (offset % mapedFileSize);
            SelectMapedBufferResult result = mapedFile.selectMapedBuffer(pos);
            return result;
        }

        return null;
    }


    /**
     *
     * 当正常退出时，数据恢复，所有内存数据已刷盘
     */
    public void recoverNormally() {
        // 重启时，是否校验CRC
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        final List<MapedFile> mapedFiles = this.mapedFileQueue.getMapedFiles();
        if (!mapedFiles.isEmpty()) {
            // 从倒数第三个文件开始恢复
            int index = mapedFiles.size() - 3;
            if (index < 0)
                index = 0;

            MapedFile mapedFile = mapedFiles.get(index);
            ByteBuffer byteBuffer = mapedFile.sliceByteBuffer();
            long processOffset = mapedFile.getFileFromOffset();// 起始偏移
            long mapedFileOffset = 0;
            // 循环查找文件中的每个消息
            while (true) {
                DispatchRequest dispatchRequest =
                        this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
                int size = dispatchRequest.getMsgSize();
                // 正常的消息
                if (size > 0) {
                    mapedFileOffset += size;
                }
                // 文件读取错误
                else if (size == -1) {
                    log.info("recover physics file end, " + mapedFile.getFileName());
                    break;
                }
                // 走到这里表示某个文件中的所有消息已经读完了
                else if (size == 0) {
                    index++;
                    if (index >= mapedFiles.size()) {
                        // 文件中的数据恢复完成
                        log.info(
                            "recover last 3 physics file over, last maped file " + mapedFile.getFileName());
                        break;
                    }
                    else {
                        // 读取下一个文件
                        mapedFile = mapedFiles.get(index);
                        byteBuffer = mapedFile.sliceByteBuffer();
                        processOffset = mapedFile.getFileFromOffset();
                        mapedFileOffset = 0;
                        log.info("recover next physics file, " + mapedFile.getFileName());
                    }
                }
            }
            processOffset += mapedFileOffset;
            this.mapedFileQueue.setCommittedWhere(processOffset);
            // 清除脏数据
            this.mapedFileQueue.truncateDirtyFiles(processOffset);
        }
    }


    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC) {
        return this.checkMessageAndReturnSize(byteBuffer, checkCRC, true);
    }


    /**
     * 查找消息并返回消息的长度
     * 
     * @return 没找到 -1 文件结束 0
     */
    public DispatchRequest checkMessageAndReturnSize(ByteBuffer byteBuffer, final boolean checkCRC,
            final boolean readBody) {
        try {
            //
            ByteBuffer byteBufferMessage =
                    ((DefaultAppendMessageCallback) this.appendMessageCallback).getMsgStoreItemMemory();
            byte[] bytesContent = byteBufferMessage.array();

            // 1 TOTALSIZE 总大小
            int totalSize = byteBuffer.getInt();

            // 2 MAGICCODE
            int magicCode = byteBuffer.getInt();
            switch (magicCode) {
            case MessageMagicCode:
                break;
            case BlankMagicCode:
                return new DispatchRequest(0);
            default:
                log.warn("found a illegal magic code 0x" + Integer.toHexString(magicCode));
                return new DispatchRequest(-1);
            }

            // 3 BODYCRC
            int bodyCRC = byteBuffer.getInt();

            // 4 QUEUEID
            int queueId = byteBuffer.getInt();

            // 5 FLAG
            int flag = byteBuffer.getInt();
            flag = flag + 0;

            // 6 QUEUEOFFSET
            long queueOffset = byteBuffer.getLong();

            // 7 PHYSICALOFFSET
            long physicOffset = byteBuffer.getLong();

            // 8 SYSFLAG
            int sysFlag = byteBuffer.getInt();

            // 9 BORNTIMESTAMP
            long bornTimeStamp = byteBuffer.getLong();
            bornTimeStamp = bornTimeStamp + 0;

            // 10 BORNHOST（IP+PORT）
            byteBuffer.get(bytesContent, 0, 8);

            // 11 STORETIMESTAMP
            long storeTimestamp = byteBuffer.getLong();

            // 12 STOREHOST（IP+PORT）
            byteBuffer.get(bytesContent, 0, 8);

            // 13 RECONSUMETIMES
            int reconsumeTimes = byteBuffer.getInt();

            // 14 Prepared Transaction Offset
            long preparedTransactionOffset = byteBuffer.getLong();

            // 15 BODY
            int bodyLen = byteBuffer.getInt();
            if (bodyLen > 0) {
                if (readBody) {
                    byteBuffer.get(bytesContent, 0, bodyLen);

                    if (checkCRC) {
                        int crc = UtilAll.crc32(bytesContent, 0, bodyLen);
                        if (crc != bodyCRC) {
                            log.warn("CRC check failed " + crc + " " + bodyCRC);
                            return new DispatchRequest(-1);
                        }
                    }
                }
                else {
                    byteBuffer.position(byteBuffer.position() + bodyLen);
                }
            }

            // 16 TOPIC
            byte topicLen = byteBuffer.get();
            byteBuffer.get(bytesContent, 0, topicLen);
            String topic = new String(bytesContent, 0, topicLen);

            long tagsCode = 0;
            String keys = "";

            // 17 properties
            short propertiesLength = byteBuffer.getShort();
            if (propertiesLength > 0) {
                byteBuffer.get(bytesContent, 0, propertiesLength);
                String properties = new String(bytesContent, 0, propertiesLength);
                Map<String, String> propertiesMap = MessageDecoder.string2messageProperties(properties);

                keys = propertiesMap.get(MessageConst.PROPERTY_KEYS);
                String tags = propertiesMap.get(MessageConst.PROPERTY_TAGS);
                if (tags != null && tags.length() > 0) {
                    tagsCode = MessageExtBrokerInner
                        .tagsString2tagsCode(MessageExt.parseTopicFilterType(sysFlag), tags);
                }

                // Timing message processing
                {
                    String t = propertiesMap.get(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
                    if (ScheduleMessageService.SCHEDULE_TOPIC.equals(topic) && t != null) {
                        int delayLevel = Integer.parseInt(t);

                        if (delayLevel > this.defaultMessageStore.getScheduleMessageService()
                            .getMaxDelayLevel()) {
                            delayLevel =
                                    this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel();
                        }

                        if (delayLevel > 0) {
                            tagsCode = this.defaultMessageStore.getScheduleMessageService()
                                .computeDeliverTimestamp(delayLevel, storeTimestamp);
                        }
                    }
                }
            }

            return new DispatchRequest(//
                topic, // 1
                queueId, // 2
                physicOffset, // 3
                totalSize, // 4
                tagsCode, // 5
                storeTimestamp, // 6
                queueOffset, // 7
                keys, // 8
                sysFlag, // 9
                preparedTransactionOffset// 10
            );
        }
        catch (BufferUnderflowException e) {
            byteBuffer.position(byteBuffer.limit());
        }
        catch (Exception e) {
            byteBuffer.position(byteBuffer.limit());
        }

        return new DispatchRequest(-1);
    }


    /**
     * 非正常退出恢复
     */
    public void recoverAbnormally() {
        // 用最小时间戳恢复
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        final List<MapedFile> mapedFiles = this.mapedFileQueue.getMapedFiles();
        if (!mapedFiles.isEmpty()) {
            // 找到最小时间戳的那个文件
            int index = mapedFiles.size() - 1;
            MapedFile mapedFile = null;
            for (; index >= 0; index--) {
                mapedFile = mapedFiles.get(index);
                if (this.isMapedFileMatchedRecover(mapedFile)) {
                    log.info("recover from this maped file " + mapedFile.getFileName());
                    break;
                }
            }

            if (index < 0) {
                index = 0;
                mapedFile = mapedFiles.get(index);
            }

            ByteBuffer byteBuffer = mapedFile.sliceByteBuffer();
            long processOffset = mapedFile.getFileFromOffset();
            long mapedFileOffset = 0;
            while (true) {
                DispatchRequest dispatchRequest =
                        this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
                int size = dispatchRequest.getMsgSize();
                // Normal data
                if (size > 0) {
                    mapedFileOffset += size;
                    this.defaultMessageStore.putDispatchRequest(dispatchRequest);
                }
                // Intermediate file read error
                else if (size == -1) {
                    log.info("recover physics file end, " + mapedFile.getFileName());
                    break;
                }
                // Come the end of the file, switch to the next file
                // Since the return 0 representatives met last hole, this can
                // not be included in truncate offset
                else if (size == 0) {
                    index++;
                    if (index >= mapedFiles.size()) {
                        // The current branch under normal circumstances should
                        // not happen
                        log.info("recover physics file over, last maped file " + mapedFile.getFileName());
                        break;
                    }
                    else {
                        mapedFile = mapedFiles.get(index);
                        byteBuffer = mapedFile.sliceByteBuffer();
                        processOffset = mapedFile.getFileFromOffset();
                        mapedFileOffset = 0;
                        log.info("recover next physics file, " + mapedFile.getFileName());
                    }
                }
            }

            processOffset += mapedFileOffset;
            this.mapedFileQueue.setCommittedWhere(processOffset);
            this.mapedFileQueue.truncateDirtyFiles(processOffset);// 清除无效数据

            // 清除消费者队列多余的数据
            this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
        }
        // 没有commitlog文件
        else {
            this.mapedFileQueue.setCommittedWhere(0);
            this.defaultMessageStore.destroyLogics();
        }
    }


    // 找到匹配的恢复文件
    private boolean isMapedFileMatchedRecover(final MapedFile mapedFile) {
        ByteBuffer byteBuffer = mapedFile.sliceByteBuffer();
        // 根据判断文件中是否存在消息
        int magicCode = byteBuffer.getInt(MessageDecoder.MessageMagicCodePostion);
        if (magicCode != MessageMagicCode) {
            return false;
        }

        long storeTimestamp = byteBuffer.getLong(MessageDecoder.MessageStoreTimestampPostion);
        if (0 == storeTimestamp) {
            return false;
        }
        // 判断是否开启消息索引功能和是否使用安全的消息索引功能，即可靠模式
        if (this.defaultMessageStore.getMessageStoreConfig().isMessageIndexEnable()//
                && this.defaultMessageStore.getMessageStoreConfig().isMessageIndexSafe()) {

            if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestampIndex()) {
                log.info("find check timestamp, {} {}", //
                    storeTimestamp, //
                    UtilAll.timeMillisToHumanString(storeTimestamp));
                return true;
            }
        }
        else {
            if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestamp()) {
                log.info("find check timestamp, {} {}", //
                    storeTimestamp, //
                    UtilAll.timeMillisToHumanString(storeTimestamp));
                return true;
            }
        }

        return false;
    }


    /**
     * 将接收到的消息写到文件中
     *
     * @param msg
     * @return
     */
    public PutMessageResult putMessage(final MessageExtBrokerInner msg) {
        // 设置消息存储的是时间戳
        msg.setStoreTimestamp(System.currentTimeMillis());
        /**
         * CRC32表示使用32位来保存错误检查信息，在创建镜像文件的时候，CRC32的详细信息将嵌入到该文件中，
         * 以确保在将该文件还原到磁盘时能够检测到它是否已经损坏。
         */
        msg.setBodyCRC(UtilAll.crc32(msg.getBody()));
        // 写入结果
        AppendMessageResult result = null;

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        String topic = msg.getTopic();
        int queueId = msg.getQueueId();
        long tagsCode = msg.getTagsCode();

        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
        if (tranType == MessageSysFlag.TransactionNotType//
                || tranType == MessageSysFlag.TransactionCommitType) {
            // 息延时投递时间级别
            if (msg.getDelayTimeLevel() > 0) {// 大于0表示特定延时级别（具体级别在服务器端定义）
                if (msg.getDelayTimeLevel() > this.defaultMessageStore.getScheduleMessageService()
                    .getMaxDelayLevel()) {
                    msg.setDelayTimeLevel(
                        this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel());
                }

                topic = ScheduleMessageService.SCHEDULE_TOPIC;
                queueId = ScheduleMessageService.delayLevel2QueueId(msg.getDelayTimeLevel());
                tagsCode = this.defaultMessageStore.getScheduleMessageService()
                    .computeDeliverTimestamp(msg.getDelayTimeLevel(), msg.getStoreTimestamp());

                // Backup real topic, queueId
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_TOPIC, msg.getTopic());
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_QUEUE_ID,
                    String.valueOf(msg.getQueueId()));
                msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

                msg.setTopic(topic);
                msg.setQueueId(queueId);
            }
        }

        long eclipseTimeInLock = 0;
        synchronized (this) {
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();

            // 这里设置存储时间戳，以确保有序
            // global
            msg.setStoreTimestamp(beginLockTimestamp);
            // 获取最新的mapedfile
            MapedFile mapedFile = this.mapedFileQueue.getLastMapedFile();
            if (null == mapedFile) {
                log.error("create maped file1 error, topic: " + msg.getTopic() + " clientAddr: "
                        + msg.getBornHostString());
                return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null);
            }
            result = mapedFile.appendMessage(msg, this.appendMessageCallback);
            switch (result.getStatus()) {
            case PUT_OK:
                break;
            case END_OF_FILE:
                // Create a new file, re-write the message
                mapedFile = this.mapedFileQueue.getLastMapedFile();
                if (null == mapedFile) {
                    // XXX: warn and notify me
                    log.error("create maped file2 error, topic: " + msg.getTopic() + " clientAddr: "
                            + msg.getBornHostString());
                    return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result);
                }
                result = mapedFile.appendMessage(msg, this.appendMessageCallback);
                break;
            case MESSAGE_SIZE_EXCEEDED:
                return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result);
            case UNKNOWN_ERROR:
                return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
            default:
                return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
            }

            DispatchRequest dispatchRequest = new DispatchRequest(//
                topic, // 1
                queueId, // 2
                result.getWroteOffset(), // 3
                result.getWroteBytes(), // 4
                tagsCode, // 5
                msg.getStoreTimestamp(), // 6
                result.getLogicsOffset(), // 7
                msg.getKeys(), // 8
                /**
                 * Transaction
                 */
                msg.getSysFlag(), // 9
                msg.getPreparedTransactionOffset());// 10

            this.defaultMessageStore.putDispatchRequest(dispatchRequest);

            eclipseTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
        } // end of synchronized

        if (eclipseTimeInLock > 1000) {
            // XXX: warn and notify me
            log.warn("putMessage in lock eclipse time(ms) " + eclipseTimeInLock);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        storeStatsService.getSinglePutMessageTopicSizeTotal(topic).addAndGet(result.getWroteBytes());

        GroupCommitRequest request = null;

        // 同步刷盘
        if (FlushDiskType.SYNC_FLUSH == this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
            GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
            if (msg.isWaitStoreMsgOK()) {
                request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes());
                service.putRequest(request);
                boolean flushOK = request
                    .waitForFlush(this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
                if (!flushOK) {
                    log.error("do groupcommit, wait for flush failed, topic: " + msg.getTopic() + " tags: "
                            + msg.getTags() + " client address: " + msg.getBornHostString());
                    putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_DISK_TIMEOUT);
                }
            }
            else {
                service.wakeup();
            }
        }
        // 异步刷盘
        else {
            this.flushCommitLogService.wakeup();
        }

        // 同步将消息写入从节点
        if (BrokerRole.SYNC_MASTER == this.defaultMessageStore.getMessageStoreConfig().getBrokerRole()) {
            HAService service = this.defaultMessageStore.getHaService();
            if (msg.isWaitStoreMsgOK()) {
                // Determine whether to wait
                if (service.isSlaveOK(result.getWroteOffset() + result.getWroteBytes())) {
                    if (null == request) {
                        request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes());
                    }
                    service.putRequest(request);

                    service.getWaitNotifyObject().wakeupAll();

                    boolean flushOK =
                            // TODO
                            request.waitForFlush(
                                this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
                    if (!flushOK) {
                        log.error("do sync transfer other node, wait return, but failed, topic: "
                                + msg.getTopic() + " tags: " + msg.getTags() + " client address: "
                                + msg.getBornHostString());
                        putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
                    }
                }
                // Slave problem
                else {
                    // Tell the producer, slave not available
                    putMessageResult.setPutMessageStatus(PutMessageStatus.SLAVE_NOT_AVAILABLE);
                }
            }
        }

        return putMessageResult;
    }


    /**
     * According to receive certain message or offset storage time if an error
     * occurs, it returns -1
     */
    public long pickupStoretimestamp(final long offset, final int size) {
        if (offset > this.getMinOffset()) {
            SelectMapedBufferResult result = this.getMessage(offset, size);
            if (null != result) {
                try {
                    return result.getByteBuffer().getLong(MessageDecoder.MessageStoreTimestampPostion);
                }
                finally {
                    result.release();
                }
            }
        }

        return -1;
    }


    public SelectMapedBufferResult getMessage(final long offset, final int size) {
        int mapedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog();
        MapedFile mapedFile = this.mapedFileQueue.findMapedFileByOffset(offset, (0 == offset ? true : false));
        if (mapedFile != null) {
            int pos = (int) (offset % mapedFileSize);
            SelectMapedBufferResult result = mapedFile.selectMapedBuffer(pos, size);
            return result;
        }

        return null;
    }


    public HashMap<String, Long> getTopicQueueTable() {
        return topicQueueTable;
    }


    public void setTopicQueueTable(HashMap<String, Long> topicQueueTable) {
        this.topicQueueTable = topicQueueTable;
    }


    public void destroy() {
        this.mapedFileQueue.destroy();
    }


    public boolean appendData(long startOffset, byte[] data) {
        synchronized (this) {
            MapedFile mapedFile = this.mapedFileQueue.getLastMapedFile(startOffset);
            if (null == mapedFile) {
                log.error("appendData getLastMapedFile error  " + startOffset);
                return false;
            }

            return mapedFile.appendMessage(data);
        }
    }


    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        return this.mapedFileQueue.retryDeleteFirstFile(intervalForcibly);
    }

    // 刷盘服务线程
    abstract class FlushCommitLogService extends ServiceThread {
    }

    // 实时刷盘服务
    class FlushRealTimeService extends FlushCommitLogService {
        private static final int RetryTimesOver = 3;
        private long lastFlushTimestamp = 0;
        private long printTimes = 0;


        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStoped()) {
                // 是否定时方式刷盘
                boolean flushCommitLogTimed =
                        CommitLog.this.defaultMessageStore.getMessageStoreConfig().isFlushCommitLogTimed();
                // CommitLog刷盘间隔时间（单位毫秒）
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig()
                    .getFlushIntervalCommitLog();
                // 刷CommitLog，至少刷几个PAGE
                int flushPhysicQueueLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig()
                    .getFlushCommitLogLeastPages();
                // 刷CommitLog，彻底刷盘间隔时间
                int flushPhysicQueueThoroughInterval = CommitLog.this.defaultMessageStore
                    .getMessageStoreConfig().getFlushCommitLogThoroughInterval();

                boolean printFlushProgress = false;

                // Print flush progress
                long currentTimeMillis = System.currentTimeMillis();
                if (currentTimeMillis >= (this.lastFlushTimestamp + flushPhysicQueueThoroughInterval)) {
                    this.lastFlushTimestamp = currentTimeMillis;
                    flushPhysicQueueLeastPages = 0;
                    printFlushProgress = ((printTimes++ % 10) == 0);
                }

                try {
                    if (flushCommitLogTimed) {
                        Thread.sleep(interval);
                    }
                    else {
                        this.waitForRunning(interval);
                    }

                    if (printFlushProgress) {
                        this.printFlushProgress();
                    }
                    // 刷盘
                    CommitLog.this.mapedFileQueue.commit(flushPhysicQueueLeastPages);
                    long storeTimestamp = CommitLog.this.mapedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint()
                            .setPhysicMsgTimestamp(storeTimestamp);
                    }
                }
                catch (Exception e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                    this.printFlushProgress();
                }
            }

            // Normal shutdown, to ensure that all the flush before exit
            boolean result = false;
            for (int i = 0; i < RetryTimesOver && !result; i++) {
                result = CommitLog.this.mapedFileQueue.commit(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times "
                        + (result ? "OK" : "Not OK"));
            }

            this.printFlushProgress();

            CommitLog.log.info(this.getServiceName() + " service end");
        }


        @Override
        public String getServiceName() {
            return FlushCommitLogService.class.getSimpleName();
        }


        private void printFlushProgress() {
            CommitLog.log.info(
                "how much disk fall behind memory, " + CommitLog.this.mapedFileQueue.howMuchFallBehind());
        }


        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
    }

    public class GroupCommitRequest {
        private final long nextOffset;
        private final CountDownLatch countDownLatch = new CountDownLatch(1);
        private volatile boolean flushOK = false;


        public GroupCommitRequest(long nextOffset) {
            this.nextOffset = nextOffset;
        }


        public long getNextOffset() {
            return nextOffset;
        }


        public void wakeupCustomer(final boolean flushOK) {
            this.flushOK = flushOK;
            this.countDownLatch.countDown();
        }


        public boolean waitForFlush(long timeout) {
            try {
                boolean result = this.countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
                return result || this.flushOK;
            }
            catch (InterruptedException e) {
                e.printStackTrace();
                return false;
            }
        }
    }

    /**
     * GroupCommit Service
     */
    class GroupCommitService extends FlushCommitLogService {
        private volatile List<GroupCommitRequest> requestsWrite = new ArrayList<GroupCommitRequest>();
        private volatile List<GroupCommitRequest> requestsRead = new ArrayList<GroupCommitRequest>();


        public void putRequest(final GroupCommitRequest request) {
            synchronized (this) {
                this.requestsWrite.add(request);
                if (!this.hasNotified) {
                    this.hasNotified = true;
                    this.notify();
                }
            }
        }


        private void swapRequests() {
            List<GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }


        private void doCommit() {
            if (!this.requestsRead.isEmpty()) {
                for (GroupCommitRequest req : this.requestsRead) {
                    // There may be a message in the next file, so a maximum of
                    // two times the flush
                    boolean flushOK = false;
                    for (int i = 0; (i < 2) && !flushOK; i++) {
                        flushOK = (CommitLog.this.mapedFileQueue.getCommittedWhere() >= req.getNextOffset());

                        if (!flushOK) {
                            CommitLog.this.mapedFileQueue.commit(0);
                        }
                    }

                    req.wakeupCustomer(flushOK);
                }

                long storeTimestamp = CommitLog.this.mapedFileQueue.getStoreTimestamp();
                if (storeTimestamp > 0) {
                    CommitLog.this.defaultMessageStore.getStoreCheckpoint()
                        .setPhysicMsgTimestamp(storeTimestamp);
                }

                this.requestsRead.clear();
            }
            else {
                // Because of individual messages is set to not sync flush, it
                // will come to this process
                CommitLog.this.mapedFileQueue.commit(0);
            }
        }


        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStoped()) {
                try {
                    this.waitForRunning(0);
                    this.doCommit();
                }
                catch (Exception e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // 线程停止 休眠10毫秒 再刷一次盘
            try {
                Thread.sleep(10);
            }
            catch (InterruptedException e) {
                CommitLog.log.warn("GroupCommitService Exception, ", e);
            }

            synchronized (this) {
                this.swapRequests();
            }

            this.doCommit();

            CommitLog.log.info(this.getServiceName() + " service end");
        }


        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }


        @Override
        public String getServiceName() {
            return GroupCommitService.class.getSimpleName();
        }


        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
    }

    class DefaultAppendMessageCallback implements AppendMessageCallback {
        // 文件结束时的最小固定长度为空
        private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;
        // 消息id
        private final ByteBuffer msgIdMemory;
        // 存储在内存中的消息
        private final ByteBuffer msgStoreItemMemory;
        // 消息的最大长度
        private final int maxMessageSize;


        DefaultAppendMessageCallback(final int size) {
            this.msgIdMemory = ByteBuffer.allocate(MessageDecoder.MSG_ID_LENGTH);
            this.msgStoreItemMemory = ByteBuffer.allocate(size + END_FILE_MIN_BLANK_LENGTH);
            this.maxMessageSize = size;
        }


        public ByteBuffer getMsgStoreItemMemory() {
            return msgStoreItemMemory;
        }


        /**
         * @param fileFromOffset
         * @param byteBuffer
         * @param maxBlank
         * @param msg
         * @return
         */
        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer,
                final int maxBlank, final Object msg) {
            // 封装消息的类
            MessageExtBrokerInner msgInner = (MessageExtBrokerInner) msg;
            // 从哪里开始写
            long wroteOffset = fileFromOffset + byteBuffer.position();
            // 创建消息id
            String msgId = MessageDecoder.createMessageId(this.msgIdMemory, msgInner.getStoreHostBytes(),
                wroteOffset);

            // 消息的消费队列---》偏移量
            String key = msgInner.getTopic() + "-" + msgInner.getQueueId();
            Long queueOffset = CommitLog.this.topicQueueTable.get(key);
            if (null == queueOffset) {
                queueOffset = 0L;
                CommitLog.this.topicQueueTable.put(key, queueOffset);
            }

            // 需要特殊处理的事务消息
            final int tranType = MessageSysFlag.getTransactionValue(msgInner.getSysFlag());
            switch (tranType) {
            // 准备和回滚消息不被消耗，不会进入消费队列
            case MessageSysFlag.TransactionPreparedType:
            case MessageSysFlag.TransactionRollbackType:
                queueOffset = 0L;
                break;
            case MessageSysFlag.TransactionNotType:
            case MessageSysFlag.TransactionCommitType:
            default:
                break;
            }

            /**
             * Serialize message
             */
            final byte[] propertiesData =
                    msgInner.getPropertiesString() == null ? null : msgInner.getPropertiesString().getBytes();
            final int propertiesLength = propertiesData == null ? 0 : propertiesData.length;

            final byte[] topicData = msgInner.getTopic().getBytes();
            final int topicLength = topicData == null ? 0 : topicData.length;

            final int bodyLength = msgInner.getBody() == null ? 0 : msgInner.getBody().length;
            // 消息的真实长度
            final int msgLen = 4 // 1 TOTALSIZE
                    + 4 // 2 MAGICCODE
                    + 4 // 3 BODYCRC
                    + 4 // 4 QUEUEID
                    + 4 // 5 FLAG
                    + 8 // 6 QUEUEOFFSET
                    + 8 // 7 PHYSICALOFFSET
                    + 4 // 8 SYSFLAG
                    + 8 // 9 BORNTIMESTAMP
                    + 8 // 10 BORNHOST
                    + 8 // 11 STORETIMESTAMP
                    + 8 // 12 STOREHOSTADDRESS
                    + 4 // 13 RECONSUMETIMES
                    + 8 // 14 Prepared Transaction Offset
                    + 4 + bodyLength // 14 BODY
                    + 1 + topicLength // 15 TOPIC
                    + 2 + propertiesLength // 16 propertiesLength
                    + 0;

            // 判断消息字节是否超限
            if (msgLen > this.maxMessageSize) {
                CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: "
                        + bodyLength + ", maxMessageSize: " + this.maxMessageSize);
                return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
            }

            // 判断buffer的剩余空间是否能存的下这个消息
            // 如果剩余空间不足，则向buffer中写入一个结束标志BlankMagicCode
            if ((msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                // 从新设置buffer的大小
                this.resetMsgStoreItemMemory(maxBlank);
                // 1 TOTALSIZE
                this.msgStoreItemMemory.putInt(maxBlank);
                // 2 MAGICCODE
                this.msgStoreItemMemory.putInt(CommitLog.BlankMagicCode);
                // 3 The remaining space may be any value
                //

                // Here the length of the specially set maxBlank
                byteBuffer.put(this.msgStoreItemMemory.array(), 0, maxBlank);
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgId,
                    msgInner.getStoreTimestamp(), queueOffset);
            }

            // Initialization of storage space
            this.resetMsgStoreItemMemory(msgLen);
            // 1 TOTALSIZE
            this.msgStoreItemMemory.putInt(msgLen);
            // 2 MAGICCODE
            this.msgStoreItemMemory.putInt(CommitLog.MessageMagicCode);
            // 3 BODYCRC
            this.msgStoreItemMemory.putInt(msgInner.getBodyCRC());
            // 4 QUEUEID
            this.msgStoreItemMemory.putInt(msgInner.getQueueId());
            // 5 FLAG
            this.msgStoreItemMemory.putInt(msgInner.getFlag());
            // 6 QUEUEOFFSET
            this.msgStoreItemMemory.putLong(queueOffset);
            // 7 PHYSICALOFFSET
            this.msgStoreItemMemory.putLong(fileFromOffset + byteBuffer.position());
            // 8 SYSFLAG
            this.msgStoreItemMemory.putInt(msgInner.getSysFlag());
            // 9 BORNTIMESTAMP
            this.msgStoreItemMemory.putLong(msgInner.getBornTimestamp());
            // 10 BORNHOST
            this.msgStoreItemMemory.put(msgInner.getBornHostBytes());
            // 11 STORETIMESTAMP
            this.msgStoreItemMemory.putLong(msgInner.getStoreTimestamp());
            // 12 STOREHOSTADDRESS
            this.msgStoreItemMemory.put(msgInner.getStoreHostBytes());
            // 13 RECONSUMETIMES
            this.msgStoreItemMemory.putInt(msgInner.getReconsumeTimes());
            // 14 Prepared Transaction Offset
            this.msgStoreItemMemory.putLong(msgInner.getPreparedTransactionOffset());
            // 15 BODY
            this.msgStoreItemMemory.putInt(bodyLength);
            if (bodyLength > 0)
                this.msgStoreItemMemory.put(msgInner.getBody());
            // 16 TOPIC
            this.msgStoreItemMemory.put((byte) topicLength);
            this.msgStoreItemMemory.put(topicData);
            // 17 PROPERTIES
            this.msgStoreItemMemory.putShort((short) propertiesLength);
            if (propertiesLength > 0)
                this.msgStoreItemMemory.put(propertiesData);

            // 将消息写入缓冲区（文件）
            byteBuffer.put(this.msgStoreItemMemory.array(), 0, msgLen);

            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset,
                msgLen, msgId, msgInner.getStoreTimestamp(), queueOffset);

            switch (tranType) {
            case MessageSysFlag.TransactionPreparedType:
            case MessageSysFlag.TransactionRollbackType:
                break;
            case MessageSysFlag.TransactionNotType:
            case MessageSysFlag.TransactionCommitType:
                // 更新topic队列
                CommitLog.this.topicQueueTable.put(key, ++queueOffset);
                break;
            default:
                break;
            }

            return result;
        }


        // 根据消息的实际大小从新设置buffer的大小
        private void resetMsgStoreItemMemory(final int length) {
            this.msgStoreItemMemory.flip();
            this.msgStoreItemMemory.limit(length);
        }
    }


    public void removeQueurFromTopicQueueTable(final String topic, final int queueId) {
        String key = topic + "-" + queueId;
        synchronized (this) {
            this.topicQueueTable.remove(key);
        }

        log.info("removeQueurFromTopicQueueTable OK Topic: {} QueueId: {}", topic, queueId);
    }
}
