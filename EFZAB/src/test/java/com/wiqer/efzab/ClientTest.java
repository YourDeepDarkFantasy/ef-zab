package com.wiqer.efzab;

import com.wiqer.efzab.client.Client;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class ClientTest {
    private Client client;

    @Before
    public void setUp() {
        client = Client.getInstance();
    }

    @After
    public void tearDown() {
//        Client.shutdown();
    }

    @Test
    public void testKVPut() {
        String key = "election";
        String value = "ZAB";
        boolean flag = client.put(key, value);
        System.out.println("Put-------====>flag: " + flag);
    }

    @Test
    public void testKVGet() {
        String key = "election";
        String value = client.get(key);
        System.out.println("Get-------====>value: " + value);
    }
}
