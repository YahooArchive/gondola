/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.impl;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.function.Function;

/**
 * The file secret helper simply read from a property file, and secret from the file.
 */
public class FileSecretHelper implements Function<String, String> {

    Properties properties;

    /**
     * Reads secret property from the secret file.
     *
     * @param secretFile secret file in Java property format
     * @throws IOException
     */
    public FileSecretHelper(String secretFile) throws IOException {
        properties = new Properties();
        try (FileInputStream fileInputStream = new FileInputStream(secretFile)) {
            properties.load(fileInputStream);
        }
    }

    @Override
    public String apply(String property) {
        return properties.getProperty(property);
    }
}
