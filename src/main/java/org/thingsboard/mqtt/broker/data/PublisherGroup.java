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
package org.thingsboard.mqtt.broker.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

@Getter
public class PublisherGroup {
    private final int id;
    private final int publishers;
    private final String topicPrefix;
    private final String clientIdPrefix;
    private final boolean debugEnabled;

    public PublisherGroup(int id, int publishers, String topicPrefix) {
        this(id, publishers, topicPrefix, null, false);
    }

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public PublisherGroup(@JsonProperty("id") int id, @JsonProperty("publishers") int publishers,
                          @JsonProperty("topicPrefix") String topicPrefix, @JsonProperty("clientIdPrefix") String clientIdPrefix,
                          @JsonProperty("isDebugEnabled") Boolean isDebugEnabled) {
        this.id = id;
        this.publishers = publishers;
        this.topicPrefix = topicPrefix;
        this.clientIdPrefix = clientIdPrefix != null ? clientIdPrefix : "test_pub_client_" + id + "_";
        this.debugEnabled = isDebugEnabled != null ? isDebugEnabled : false;
    }
}
