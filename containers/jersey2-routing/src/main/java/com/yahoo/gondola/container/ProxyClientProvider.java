/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.gondola.container;

import com.yahoo.gondola.Config;
import com.yahoo.gondola.container.client.ApacheHttpComponentProxyClient;
import com.yahoo.gondola.container.client.ProxyClient;

/**
 * Provider for proxy client.
 */
public class ProxyClientProvider {
    ProxyClient getProxyClient(Config config) {
        return new ApacheHttpComponentProxyClient();
    }
}
