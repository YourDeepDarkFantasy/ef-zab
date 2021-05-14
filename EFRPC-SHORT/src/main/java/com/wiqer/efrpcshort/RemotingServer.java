package com.wiqer.efrpcshort;

import java.util.concurrent.ExecutorService;


public interface RemotingServer extends RemotingService {
    void registerDefaultProcessor(final RequestProcessor processor, final ExecutorService executor);
}
