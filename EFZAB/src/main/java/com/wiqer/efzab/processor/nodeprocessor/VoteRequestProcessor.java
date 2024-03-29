package com.wiqer.efzab.processor.nodeprocessor;

import com.wiqer.efzab.Node;
import com.wiqer.efzab.NodeStatus;
import com.wiqer.efzab.Vote;
import com.wiqer.efzab.message.nodemsg.VoteMessage;
import com.wiqer.efrpcshort.netty.NettyRequestProcessor;
import com.wiqer.efrpcshort.protocol.RemotingMessage;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;


public class VoteRequestProcessor implements NettyRequestProcessor {
    private static final Logger logger = LogManager.getLogger(VoteRequestProcessor.class.getSimpleName());

    private final Node node;

    public VoteRequestProcessor(final Node node) {
        this.node = node;
    }

    @Override
    public RemotingMessage processRequest(ChannelHandlerContext ctx, RemotingMessage request) throws Exception {
        try {
            if (node.getVoteLock().tryLock(3000, TimeUnit.MILLISECONDS)) {
                VoteMessage resVoteMsg = VoteMessage.getInstance().parseMessage(request);
                Vote peerVote = resVoteMsg.getVote();
                logger.info("Receive peer vote: {}", peerVote);

                node.getVoteBox().put(peerVote.getNodeId(), peerVote);
                //版本号大于当前节点
                if (peerVote.getEpoch() > node.getMyVote().getEpoch()) {
                    node.getMyVote().setEpoch(peerVote.getEpoch());
                    node.getMyVote().setVoteId(peerVote.getNodeId());
                    node.setStatus(NodeStatus.LOOKING);
                }else if (peerVote.getEpoch() == node.getMyVote().getEpoch()) {
                    //分支号大于当前节点
                    if (peerVote.compareTo(node.getMyVote()) == 1) {
                        node.getMyVote().setVoteId(peerVote.getNodeId());
                        node.setStatus(NodeStatus.LOOKING);
                    }
                }

                if (node.isHalf()) {
                    logger.info("Node:{} become leader!", node.getNodeConfig().getNodeId());
                    node.becomeLeader();
                }else if (node.getStatus() == NodeStatus.LOOKING){
                    VoteMessage voteMsg = VoteMessage.getInstance();
                    voteMsg.setVote(node.getMyVote());
                    node.sendOneWayMsg(voteMsg.request());
                }
            }

            return null;
        }finally {
            node.getVoteLock().unlock();
        }
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
