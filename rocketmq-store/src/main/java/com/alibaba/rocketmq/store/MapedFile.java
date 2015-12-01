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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.constant.LoggerName;


/**
 * Pagecache文件访问封装
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public class MapedFile extends ReferenceResource {
    public static final int OS_PAGE_SIZE = 1024 * 4;
    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);
    // 当前JVM中映射的虚拟内存总大小
    private static final AtomicLong TotalMapedVitualMemory = new AtomicLong(0);
    // 当前JVM中mapedfile的个数
    private static final AtomicInteger TotalMapedFiles = new AtomicInteger(0);
    // 映射的文件名
    private final String fileName;
    // 起始偏移量（接上一个文件的，即上一个文件的起始偏移加上文件大小）
    private final long fileFromOffset;
    // 映射的文件大小，定长
    private final int fileSize;
    // 映射的文件
    private final File file;
    // 映射的内存对象，position永远不变
    private final MappedByteBuffer mappedByteBuffer;
    // 当前写到什么位置(内存中)
    private final AtomicInteger wrotePostion = new AtomicInteger(0);
    // Flush到什么位置（磁盘中）
    private final AtomicInteger committedPosition = new AtomicInteger(0);
    // 映射的FileChannel对象
    private FileChannel fileChannel;
    // 最后一条消息存储时间
    private volatile long storeTimestamp = 0;
    // 队列首标示
    private boolean firstCreateInQueue = false;


    public MapedFile(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;
        // 判断文件夹是否存在，如果不存在则创建文件夹
        ensureDirOK(this.file.getParent());

        try {
            // 随机读写文件对象
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            // 创建映射的内存对象(对应到磁盘文件，这个文件大小就是初始设置的大小，并不是随着写入的数据的大小而变化的)
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            TotalMapedVitualMemory.addAndGet(fileSize);
            TotalMapedFiles.incrementAndGet();
            ok = true;
        }
        catch (FileNotFoundException e) {
            log.error("create file channel " + this.fileName + " Failed. ", e);
            throw e;
        }
        catch (IOException e) {
            log.error("map file " + this.fileName + " Failed. ", e);
            throw e;
        }
        finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }


    /**
     * 判断文件夹是否存在，如果不存在则创建文件夹
     * 
     * @param dirName
     */
    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }


    /**
     * 清理 DirectBuffer 堆外内存缓冲区
     *
     * buffer.isDirect() 判断缓冲区是否是堆外内存
     * 
     * @param buffer
     */
    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        // 释放堆外内存
        // 通过反射执行DirectByteBuffer对象的Cleanr对象的clean方法（这个方法是用来释放堆外内存的）
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }


    // AccessController.doPrivileged意思是这个是特别的,不用做权限检查.
    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                }
                catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }


    // 反射
    private static Method method(Object target, String methodName, Class<?>[] args)
            throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        }
        catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }


    /**
     * 查找DirectByteBuffer 中的关联DirectByteBuffer 如果有就返回它，如果没有就返回本身
     * 
     * @param buffer
     * @return
     */
    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";// 一个连接到这个缓冲区的对象(即DirectByteBuffer)

        // JDK7中将DirectByteBuffer类中的viewedBuffer方法换成了attachment方法
        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }


    public static int getTotalmapedfiles() {
        return TotalMapedFiles.get();
    }


    public static long getTotalMapedVitualMemory() {
        return TotalMapedVitualMemory.get();
    }


    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }


    public String getFileName() {
        return fileName;
    }


    /**
     * 获取文件大小
     */
    public int getFileSize() {
        return fileSize;
    }


    public FileChannel getFileChannel() {
        return fileChannel;
    }


    /**
     * 向MapedBuffer追加消息<br>
     * （内存中）
     * 
     * @param msg
     *            要追加的消息
     * @param cb
     *            用来对消息进行序列化，尤其对于依赖MapedFile Offset的属性进行动态序列化
     * @return 是否成功，写入多少数据
     */
    public AppendMessageResult appendMessage(final Object msg, final AppendMessageCallback cb) {
        /**
         * 1、assert <boolean表达式> 如果<boolean表达式>为true，则程序继续执行。
         * 如果为false，则程序抛出AssertionError，并终止执行。
         */
        assert msg != null;
        assert cb != null;

        int currentPos = this.wrotePostion.get();

        // 表示有空余空间
        if (currentPos < this.fileSize) {
            // 缓冲区分片
            ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            // 位置，下一个要被读或写的元素的索引，每次读写缓冲区数据时都会改变改值，为下次读写作准备
            byteBuffer.position(currentPos);
            // 向文件中写入消息的具体实现
            AppendMessageResult result =
                    cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, msg);
            this.wrotePostion.addAndGet(result.getWroteBytes());
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }

        // 上层应用应该保证不会走到这里
        log.error("MapedFile.appendMessage return null, wrotePostion: " + currentPos + " fileSize: "
                + this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }


    /**
     * 文件起始偏移量
     */
    public long getFileFromOffset() {
        return this.fileFromOffset;
    }


    /**
     * 向存储层追加数据，一般在SLAVE存储结构中使用
     * 
     * @return 返回写入了多少数据
     */
    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePostion.get();

        // 表示有空余空间
        if ((currentPos + data.length) <= this.fileSize) {
            ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);
            byteBuffer.put(data);
            this.wrotePostion.addAndGet(data.length);
            return true;
        }

        return false;
    }


    /**
     * 消息刷盘
     * 
     * @param flushLeastPages
     *            至少刷几个page
     * @return
     */
    public int commit(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                int value = this.wrotePostion.get();
                // MappedByteBuffer.force()方法,这个方法强制操作系统将内存中的内容写入硬盘
                this.mappedByteBuffer.force();
                this.committedPosition.set(value);
                this.release();
            }
            else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
                this.committedPosition.set(this.wrotePostion.get());
            }
        }

        return this.getCommittedPosition();
    }


    public int getCommittedPosition() {
        return committedPosition.get();
    }


    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }


    /**
     * 判断是否可以刷盘（从内存写到磁盘） 1、文件已经存满可以刷
     * 
     * @param flushLeastPages
     * @return
     */
    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = this.committedPosition.get();
        int write = this.wrotePostion.get();

        // 如果当前文件已经写满，应该立刻刷盘
        if (this.isFull()) {
            return true;
        }

        // 只有未刷盘数据满足指定page数目才刷盘
        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }


    // 判断文件是否满了
    public boolean isFull() {
        return this.fileSize == this.wrotePostion.get();
    }


    /**
     * 查询存储在buffer中的数据
     * 
     * @param pos
     *            查询开始偏移量
     * @param size
     *            查询数据量的字节数
     * @return
     */
    public SelectMapedBufferResult selectMapedBuffer(int pos, int size) {
        // 有消息
        if ((pos + size) <= this.wrotePostion.get()) {
            // 从MapedBuffer读
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMapedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
            else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                        + this.fileFromOffset);
            }
        }
        // 请求参数非法
        else {
            log.warn("selectMapedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                    + ", fileFromOffset: " + this.fileFromOffset);
        }

        // 非法参数或者mmap资源已经被释放
        return null;
    }


    /**
     * 读逻辑分区
     */
    public SelectMapedBufferResult selectMapedBuffer(int pos) {
        if (pos < this.wrotePostion.get() && pos >= 0) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = this.wrotePostion.get() - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMapedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        // 非法参数或者mmap资源已经被释放
        return null;
    }


    @Override
    public boolean cleanup(final long currentRef) {
        // 如果没有被shutdown，则不可以unmap文件，否则会crash
        if (this.isAvailable()) {
            log.error(
                "this file[REF:" + currentRef + "] " + this.fileName + " have not shutdown, stop unmaping.");
            return false;
        }

        // 如果已经cleanup，再次操作会引起crash
        if (this.isCleanupOver()) {
            log.error(
                "this file[REF:" + currentRef + "] " + this.fileName + " have cleanup, do not do it again.");
            // 必须返回true
            return true;
        }

        clean(this.mappedByteBuffer);
        TotalMapedVitualMemory.addAndGet(this.fileSize * (-1));
        TotalMapedFiles.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }


    /**
     * 清理资源，destroy与调用shutdown的线程必须是同一个 删除磁盘文件
     * 
     * @return 是否被destory成功，上层调用需要对失败情况处理，失败后尝试重试
     */
    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                        + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePostion() + " M:"
                        + this.getCommittedPosition() + ", "
                        + UtilAll.computeEclipseTimeMilliseconds(beginTime));
            }
            catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        }
        else {
            log.warn("destroy maped file[REF:" + this.getRefCount() + "] " + this.fileName
                    + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }


    public int getWrotePostion() {
        return wrotePostion.get();
    }


    public void setWrotePostion(int pos) {
        this.wrotePostion.set(pos);
    }


    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }


    /**
     * 方法不能在运行时调用，不安全。只在启动时，reload已有数据时调用
     */
    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }


    public long getStoreTimestamp() {
        return storeTimestamp;
    }


    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }


    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }
}
