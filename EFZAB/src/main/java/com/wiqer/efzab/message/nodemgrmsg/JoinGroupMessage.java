package com.wiqer.efzab.message.nodemgrmsg;

import com.wiqer.efzab.data.ZxId;
import com.wiqer.efzab.message.AbstractMessage;
import com.wiqer.efzab.message.MessageType;


public class JoinGroupMessage extends AbstractMessage<JoinGroupMessage> {
    private int nodeId;
    private String host;
    private int port;
    private int nodeMgrPort;

    private Boolean success;

    private JoinGroupMessage() {}

    public static JoinGroupMessage getInstance() {
        return new JoinGroupMessage();
    }

    @Override
    public int getMessageType() {
        return MessageType.JOIN_GROUP;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("JoinGroupMessage: [");
        sb.append(" nodeId=" + nodeId);
        sb.append(" host=" + host);
        sb.append(" port=" + port);
        sb.append(" nodeMgrPort=" + nodeMgrPort);
        sb.append("]");
        return sb.toString();
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getNodeMgrPort() {
        return nodeMgrPort;
    }

    public void setNodeMgrPort(int nodeMgrPort) {
        this.nodeMgrPort = nodeMgrPort;
    }

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }
}
