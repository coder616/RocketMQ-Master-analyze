/**
 * $Id: MessageModel.java 1835 2013-05-16 02:00:50Z shijia.wxr $
 */
package com.alibaba.rocketmq.common.protocol.heartbeat;

/**
 * Message model
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public enum MessageModel {
    /**
     * broadcast 所有消费者可以同时消费相同的消息
     */
    BROADCASTING,
    /**
     * clustering 所有消费者消费的消息是串行的，每个消费者只能消费队列中的一部分消息
     */
    CLUSTERING;
}
