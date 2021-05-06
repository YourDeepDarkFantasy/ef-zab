package com.wiqer.efrpcshort.netty;

import com.wiqer.efrpcshort.RequestProcessor;
import com.wiqer.efrpcshort.protocol.RemotingMessage;
import io.netty.channel.ChannelHandlerContext;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public interface NettyRequestProcessor extends RequestProcessor {
    RemotingMessage processRequest(ChannelHandlerContext ctx, RemotingMessage request) throws Exception;
}
