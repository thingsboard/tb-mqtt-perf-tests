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
package org.thingsboard.mqtt.broker.service.orchestration;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class ClusterSynchronizerImpl implements ClusterSynchronizer {

    private final CountDownLatch latch = new CountDownLatch(1);

    @Value("${test-run.max-cluster-wait-time}")
    private int waitTime;

    @Override
    public void notifyClusterReady() {
        log.info("Resuming processing");
        latch.countDown();
    }

    @Override
    public void awaitClusterReady() {
        try {
            latch.await(waitTime, TimeUnit.SECONDS);
            log.info("Cluster is ready.");
        } catch (InterruptedException e) {
            log.warn("Failed to wait for cluster to be ready.");
        }
    }
}
