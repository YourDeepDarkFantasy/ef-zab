package com.wiqer.efzab.message;

import com.wiqer.efrpcshort.protocol.JSONSerializable;
import com.wiqer.efrpcshort.protocol.RemotingMessage;
import com.wiqer.efrpcshort.protocol.RemotingMessageHeader;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public abstract class AbstractMessage<T> {

    public RemotingMessage request() {
        RemotingMessageHeader header = new RemotingMessageHeader();
        header.setCode(getMessageType());

        byte[] body = JSONSerializable.encode(this);
        RemotingMessage remotingMessage = new RemotingMessage(header, body);
        return remotingMessage;
    }

    public RemotingMessage response(final RemotingMessage request) {
        byte[] body = JSONSerializable.encode(this);

        RemotingMessage remotingMessage = new RemotingMessage(request.getMessageHeader(), body);
        return remotingMessage;
    }

    public T parseMessage(final RemotingMessage remotingMessage) {
        return (T) JSONSerializable.decode(remotingMessage.getMessageBody(), this.getClass());
    }

    public abstract int getMessageType();
}
