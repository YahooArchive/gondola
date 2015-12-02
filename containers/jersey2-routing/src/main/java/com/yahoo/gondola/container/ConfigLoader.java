/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import java.io.File;
import java.net.URI;
import java.net.URL;

/**
 * The type Config loader.
 */
public class ConfigLoader {

    /**
     * Load config file.
     *
     * @param configUri the config uri
     * @return the file
     */
    public File loadConfig(URI configUri) {
        switch (configUri.getScheme()) {
            case "classpath":
                URL resource = getClass().getClassLoader().getResource(configUri.getPath());
                if (resource == null) {
                    throw new IllegalArgumentException("File not found in " + configUri.toString());
                }
                return new File(resource.getFile());
            case "file":
                return new File(configUri.getPath());
            case "zookeeper":
                return null;
            default:
                throw new IllegalArgumentException("Scheme not supported - " + configUri.getScheme());
        }
    }
}
