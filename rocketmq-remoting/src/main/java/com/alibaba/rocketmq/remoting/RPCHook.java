package com.alibaba.rocketmq.remoting;

import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;


/**
 * rpc调用hook
 */
public interface RPCHook {
    /**
     * 在发起请求前执行
     *
     * @param remoteAddr
     *            远程ip地址
     * @param request
     *            请求的数据（RemotingCommand对象封装）
     */
    public void doBeforeRequest(final String remoteAddr, final RemotingCommand request);


    /**
     * 接收到响应后执行
     *
     * @param remoteAddr
     *            远程ip地址
     * @param request
     *            请求的数据（RemotingCommand对象封装）
     * @param response
     *            响应数据 （RemotingCommand对象封装）
     */
    public void doAfterResponse(final String remoteAddr, final RemotingCommand request,
            final RemotingCommand response);
}
