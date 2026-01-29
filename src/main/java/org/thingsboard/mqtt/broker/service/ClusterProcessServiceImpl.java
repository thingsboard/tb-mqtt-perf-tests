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

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.thingsboard.mqtt.broker.config.TestRunClusterConfig;
import org.thingsboard.mqtt.broker.config.TestRunConfiguration;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class ClusterProcessServiceImpl implements ClusterProcessService {
    private final TestRunConfiguration testRunConfiguration;
    private final TestRunClusterConfig testRunClusterConfig;

    @Value("${test-run.cluster-process-wait-time}")
    private int waitTime;

    @Override
    public <T> void process(String taskName, List<T> elements, ClusterTaskProcessor<T> processor) {
        if (elements.isEmpty()) {
            log.trace("Ignoring {} task.", taskName);
            return;
        }

        int maxOperationsPerSingleRun = testRunConfiguration.getMaxConcurrentOperations() / testRunClusterConfig.getParallelTestsCount();
        int maxConcurrentNodeOperations = maxOperationsPerSingleRun != 0 ? maxOperationsPerSingleRun : 1;

        log.info("Started processing {} with {} task.", elements.size(), taskName);
        for (int i = 0; i < elements.size(); i += maxConcurrentNodeOperations) {
            int clientsToProcess = Math.min(maxConcurrentNodeOperations, elements.size() - i);
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();

            CountDownLatch latch = new CountDownLatch(clientsToProcess);

            for (int clientIndex = i; clientIndex < i + clientsToProcess; clientIndex++) {
                T element = elements.get(clientIndex);
                processor.process(latch, element);
            }

            try {
                var result = latch.await(waitTime, TimeUnit.SECONDS);
                log.info("[{}] The result of await processing is: {}", taskName, result);
            } catch (InterruptedException e) {
                log.error("Failed to wait for the {} to finish.", taskName);
                throw new RuntimeException("Failed to wait for the " + taskName + " to finish");
            }
            stopWatch.stop();
            log.debug("Processing {} {} took {} ms.", clientsToProcess, taskName, stopWatch.getTime());
        }
        log.info("Finished processing {} task.", taskName);
    }
}
