package com.wiqer.efzab;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public class Bootstrap {
    public static void main(String[] args) {
        Node node = new Node(NodeConfig.getInstance());
        node.start();
    }
}
