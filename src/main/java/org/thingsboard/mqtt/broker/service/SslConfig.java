/**
 * Copyright © 2016-2026 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.mqtt.broker.service;

import com.google.common.io.Resources;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.security.KeyStore;

@Slf4j
@Component
public class SslConfig {
    @Value("${mqtt.ssl.enabled}")
    private boolean sslEnabled;

    @Value("${mqtt.ssl.protocol}")
    private String sslProtocol;

    @Value("${mqtt.ssl.key_store}")
    private String keyStoreFile;
    @Value("${mqtt.ssl.key_store_password}")
    private String keyStorePassword;
    @Value("${mqtt.ssl.key_password}")
    private String keyPassword;
    @Value("${mqtt.ssl.key_store_type}")
    private String keyStoreType;

    @Value("${mqtt.ssl.trust_store}")
    private String trustStoreFile;
    @Value("${mqtt.ssl.trust_store_password}")
    private String trustStorePassword;
    @Value("${mqtt.ssl.trust_store_type}")
    private String trustStoreType;

    public SslContext getSslContext() {
        if (!sslEnabled) {
            return null;
        }
        try {
            return SslContextBuilder.forClient()
                    .trustManager(initTrustStore())
                    .keyManager(initKeyStore())
                    .protocols(sslProtocol).build();
        } catch (Exception e) {
            log.warn("Failed to create SslContext", e);
            return null;
        }
    }

    private KeyManagerFactory initKeyStore() throws Exception {
        File ksFile = getFile(keyStoreFile);
        KeyStore ks = KeyStore.getInstance(keyStoreType);
        try (InputStream ksFileInputStream = new FileInputStream(ksFile)) {
            ks.load(ksFileInputStream, keyStorePassword.toCharArray());
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, keyPassword.toCharArray());
        return kmf;
    }

    private TrustManagerFactory initTrustStore() throws Exception {
        if (StringUtils.isEmpty(trustStoreFile)){
            return null;
        }
        File tsFile = getFile(trustStoreFile);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        KeyStore trustStore = KeyStore.getInstance(trustStoreType);
        try (InputStream tsFileInputStream = new FileInputStream(tsFile)) {
            trustStore.load(tsFileInputStream, trustStorePassword.toCharArray());
        }
        tmf.init(trustStore);
        return tmf;
    }

    private static File getFile(String filePath) {
        try {
            URL url = Resources.getResource(filePath);
            return new File(url.toURI());
        } catch (Exception e) {
            return new File(filePath);
        }
    }
}
