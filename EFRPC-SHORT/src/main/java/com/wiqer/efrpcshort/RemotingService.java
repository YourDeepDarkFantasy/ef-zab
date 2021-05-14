package com.wiqer.efrpcshort;

import java.util.concurrent.ExecutorService;


public interface RemotingService {
    void start();
    void shutdown();
    /*注册处理器*/
    void registerProcessor(final int requestCode, final RequestProcessor processor, final ExecutorService executor);

}
