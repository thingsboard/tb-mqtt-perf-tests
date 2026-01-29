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
package org.thingsboard.mqtt.broker.util;

import org.thingsboard.mqtt.broker.config.TestRunClusterConfig;
import org.thingsboard.mqtt.broker.config.TestRunConfiguration;
import org.thingsboard.mqtt.broker.data.PreConnectedSubscriberInfo;
import org.thingsboard.mqtt.broker.data.SubscriberGroup;

import java.util.ArrayList;
import java.util.List;

public class TestClusterUtil {
    public static List<PreConnectedSubscriberInfo> getTestNodeSubscribers(TestRunConfiguration testRunConfiguration, TestRunClusterConfig testRunClusterConfig){
        List<PreConnectedSubscriberInfo> preConnectedSubscriberInfos = new ArrayList<>();
        int currentSubscriberId = 0;
        for (SubscriberGroup subscriberGroup : testRunConfiguration.getSubscribersConfig()) {
            for (int i = 0; i < subscriberGroup.getSubscribers(); i++) {
                if (currentSubscriberId++ % testRunClusterConfig.getParallelTestsCount() == testRunClusterConfig.getSequentialNumber()) {
                    preConnectedSubscriberInfos.add(new PreConnectedSubscriberInfo(subscriberGroup, i));
                }
            }
        }
        return preConnectedSubscriberInfos;
    }

}
