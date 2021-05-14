package com.wiqer.efzab;


public class Bootstrap {
    public static void main(String[] args) {
        Node node = new Node(NodeConfig.getInstance());
        node.start();
    }
}
