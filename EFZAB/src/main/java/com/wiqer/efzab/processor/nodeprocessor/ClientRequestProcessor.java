package com.wiqer.efzab.processor.nodeprocessor;

import com.wiqer.efzab.NodeStatus;
import com.wiqer.efzab.client.KVMessage;
import com.wiqer.efzab.Node;
import com.wiqer.efrpcshort.netty.NettyRequestProcessor;
import com.wiqer.efrpcshort.protocol.RemotingMessage;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ClientRequestProcessor implements NettyRequestProcessor {
    private static final Logger logger = LogManager.getLogger(ClientRequestProcessor.class.getSimpleName());

    private final Node node;

    public ClientRequestProcessor(final Node node) {
        this.node = node;
    }

    @Override
    public RemotingMessage processRequest(ChannelHandlerContext ctx, RemotingMessage request) throws Exception {
        KVMessage kvMessage = KVMessage.getInstance().parseMessage(request);
        KVMessage.KVType kvType = kvMessage.getKvType();
        if (kvType == KVMessage.KVType.GET) {
            String value = node.getDataManager().get(kvMessage.getKey());
            if (value != null) {
                kvMessage.setValue(value);
            }else {
                kvMessage.setValue("");
            }
            kvMessage.setSuccess(true);

            logger.info("Client Get Response: " + kvMessage);
            return kvMessage.response(request);
        }

        //不是主节点，需要把消息同步到主节点，再向下执行
        if (node.getStatus() != NodeStatus.LEADING) {
            logger.info("Redirect to leader: " + node.getLeaderId());
            return node.redirect(request);
        }
        //
        if (kvType == KVMessage.KVType.PUT) {
            boolean flag = node.getDataManager().put(kvMessage.getKey(), kvMessage.getValue());
            if (flag) {
                node.getSnapshotMap().put(node.getNodeConfig().getNodeId(), true);
                //发送给所有节点
                node.appendData(kvMessage.getKey(), kvMessage.getValue());
                //通知所有节点提交数据
                boolean committed = node.commitData(kvMessage.getKey());
                kvMessage.setSuccess(committed);
            }else {
                kvMessage.setSuccess(false);
            }

            logger.info("Leader write log success!");
            return kvMessage.response(request);
        }
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
