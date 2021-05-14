package com.wiqer.efrpcshort.netty;

import com.wiqer.efrpcshort.InvokeCallback;
import com.wiqer.efrpcshort.ResponseProcessor;
import com.wiqer.efrpcshort.common.SemaphoreReleaseOnlyOnce;
import io.netty.channel.Channel;


public class NettyResponseProcessor extends ResponseProcessor {
    private final Channel processChannel;

    public NettyResponseProcessor(Channel channel, int opaque, long timeout, InvokeCallback invokeCallback, SemaphoreReleaseOnlyOnce once) {
        super(opaque, timeout, invokeCallback, once);
        this.processChannel = channel;
    }

    public Channel getProcessChannel() {
        return processChannel;
    }
}
