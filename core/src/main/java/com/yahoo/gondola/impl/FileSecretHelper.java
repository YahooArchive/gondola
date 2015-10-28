package com.yahoo.gondola.impl;

import com.yahoo.gondola.Config;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * The file secret helper simply read from a property file, and secret from the file.
 */
public class FileSecretHelper implements Config.SecretHelper {

    Properties properties;

    /**
     * Read secret property from file
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
    public String getSecret(String property) {
        return properties.getProperty(property);
    }
}
