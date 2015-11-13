/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.yahoo.gondola.Config;
import com.yahoo.gondola.container.impl.ZookeeperCommandListener;

/**
 * A Provider that provide the CommandListener implementation.
 */
public class CommandListenerProvider {

    public CommandListener getCommandListner(Config config) {
        return new ZookeeperCommandListener();
    }
}
