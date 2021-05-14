package com.wiqer.efrpcshort.netty.handler;

import com.wiqer.efrpcshort.netty.NettyRemotingClient;
import com.wiqer.efrpcshort.protocol.RemotingMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;


public class NettyClientHandler extends SimpleChannelInboundHandler<RemotingMessage> {
    private final NettyRemotingClient remotingClient;

    public NettyClientHandler(final NettyRemotingClient remotingClient) {
        this.remotingClient = remotingClient;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RemotingMessage msg) throws Exception {
        remotingClient.processMessageReceived(ctx, msg);
    }
}
