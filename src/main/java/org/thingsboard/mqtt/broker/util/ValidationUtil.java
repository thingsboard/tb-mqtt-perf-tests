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

import org.thingsboard.mqtt.broker.data.PublisherGroup;
import org.thingsboard.mqtt.broker.data.SubscriberGroup;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

public class ValidationUtil {
    public static void validateSubscriberGroups(Collection<SubscriberGroup> subscriberGroups) {
        Set<Integer> distinctIds = subscriberGroups.stream().map(SubscriberGroup::getId).collect(Collectors.toSet());
        if (distinctIds.size() != subscriberGroups.size()) {
            throw new RuntimeException("Some subscriber IDs are equal");
        }
    }

    public static void validatePublisherGroups(Collection<PublisherGroup> publisherGroups) {
        Set<Integer> distinctIds = publisherGroups.stream().map(PublisherGroup::getId).collect(Collectors.toSet());
        if (distinctIds.size() != publisherGroups.size()) {
            throw new RuntimeException("Some publisher IDs are equal");
        }
    }

}
