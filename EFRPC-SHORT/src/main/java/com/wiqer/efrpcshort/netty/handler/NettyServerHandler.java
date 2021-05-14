package com.wiqer.efrpcshort.netty.handler;

import com.wiqer.efrpcshort.netty.NettyRemotingServer;
import com.wiqer.efrpcshort.protocol.RemotingMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;


public class NettyServerHandler extends SimpleChannelInboundHandler<RemotingMessage> {
    private final NettyRemotingServer nettyRemoting;

    public NettyServerHandler(final NettyRemotingServer nettyRemoting) {
        this.nettyRemoting = nettyRemoting;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RemotingMessage msg) throws Exception {
        nettyRemoting.processMessageReceived(ctx, msg);
    }
}
