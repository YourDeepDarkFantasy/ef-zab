package com.wiqer.efrpcshort;

import java.util.concurrent.ExecutorService;

/**
 * WX: coding到灯火阑珊
 * @author Justin
 */
public interface RemotingServer extends RemotingService {
    void registerDefaultProcessor(final RequestProcessor processor, final ExecutorService executor);
}
