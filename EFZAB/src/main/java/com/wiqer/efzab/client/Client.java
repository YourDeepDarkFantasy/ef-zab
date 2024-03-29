package com.wiqer.efzab.client;

import com.wiqer.efzab.common.PropertiesUtil;
import com.wiqer.efrpcshort.RemotingClient;
import com.wiqer.efrpcshort.netty.NettyRemotingClient;
import com.wiqer.efrpcshort.netty.conf.NettyClientConfig;
import com.wiqer.efrpcshort.protocol.RemotingMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class Client {
    private static final Logger logger = LogManager.getLogger(Client.class.getSimpleName());

    private final List<String> zabNodes = new ArrayList<>();
    private final RemotingClient client;

    private Client() {
        for (String addr : PropertiesUtil.getNodesAddress().values()) {
            String[] address = addr.split(":");
            zabNodes.add(address[0]+":"+address[2]);
        }

        client = new NettyRemotingClient(new NettyClientConfig());
        client.start();
    }

    private static class ClientSingle {
        private static final Client INSTANCE = new Client();
    }

    public static Client getInstance() {
        return ClientSingle.INSTANCE;
    }

    public void shutdown() {
        client.shutdown();
    }

    public String get(String key) {
        KVMessage kvMessage = KVMessage.getInstance();
        kvMessage.setKey(key);
        kvMessage.setKvType(KVMessage.KVType.GET);

        try {
            RemotingMessage response = client.invokeSync(getRandomNodeAddr(), kvMessage.request(), 3000);
            KVMessage res = KVMessage.getInstance().parseMessage(response);
            return res.getValue();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public boolean put(String key, String value) {
        KVMessage kvMessage = KVMessage.getInstance();
        kvMessage.setKey(key);
        kvMessage.setValue(value);
        kvMessage.setKvType(KVMessage.KVType.PUT);

        try {
            RemotingMessage response = client.invokeSync(getRandomNodeAddr(), kvMessage.request(), 3000);
            KVMessage res = KVMessage.getInstance().parseMessage(response);
            return res.getSuccess();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    private String getRandomNodeAddr() {
        Random random = new Random();
        int i = random.nextInt(zabNodes.size());
        return zabNodes.get(i);
    }
}
