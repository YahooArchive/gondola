/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.impl;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URL;

import static org.testng.Assert.assertEquals;

public class FileSecretHelperTest {

    public static final String SECRET_PROPERTY = "secret.property";
    FileSecretHelper helper;

    @BeforeMethod
    public void setUp() throws Exception {
        URL resource = getClass().getClassLoader().getResource(SECRET_PROPERTY);
        if (resource != null) {
            helper = new FileSecretHelper(resource.getFile());
        } else {
            throw new IllegalStateException("test secret file not found - " + SECRET_PROPERTY);
        }
    }

    @Test
    public void testGetSecret_exists() throws Exception {
        assertEquals(helper.apply("secret"), "topSecret");
    }

    @Test
    public void testGetSecret_not_exists() throws Exception {
        assertEquals(helper.apply("foo"), null);
    }
}