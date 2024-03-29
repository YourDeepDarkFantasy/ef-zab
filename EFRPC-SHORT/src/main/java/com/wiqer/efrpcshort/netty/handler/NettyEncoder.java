package com.wiqer.efrpcshort.netty.handler;

import com.wiqer.efrpcshort.netty.event.NettyEvent;
import com.wiqer.efrpcshort.protocol.RemotingMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;


@ChannelHandler.Sharable
public class NettyEncoder extends MessageToByteEncoder<RemotingMessage> {
    private static final Logger logger = LogManager.getLogger(NettyEvent.class.getSimpleName());

    @Override
    protected void encode(ChannelHandlerContext ctx, RemotingMessage msg, ByteBuf byteBuf) throws Exception {
        try {
            ByteBuffer byteBuffer = msg.encode();
            byteBuf.writeBytes(byteBuffer);
        }catch(Exception e) {
            logger.error("encode exception " + e.getMessage());
            ctx.close();
        }
    }
}
