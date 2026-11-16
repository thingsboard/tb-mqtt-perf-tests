#
# Copyright © 2016-2024 The Thingsboard Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Prints the capacity scenario (src/main/conf/capacity_use_case.json) to stdout.

1,000 groups of 9 publishers (9,000 devices at ~1.1 msg/s, QoS 1), each group
publishing to cap/<i>/<suffix>, and 1,000 persistent DEVICE subscribers, one per
group, subscribed to cap/<i>/+.

Usage: python3 -I scripts/gen_capacity_scenario.py > src/main/conf/capacity_use_case.json
"""

import json

GROUPS = 1000
PUBLISHERS_PER_GROUP = 9


def main():
    publisher_groups = [
        {
            "id": i,
            "publishers": PUBLISHERS_PER_GROUP,
            "topicPrefix": "cap/%d/" % i,
            "clientIdPrefix": None,
        }
        for i in range(1, GROUPS + 1)
    ]
    subscriber_groups = [
        {
            "id": i,
            "subscribers": 1,
            "topicFilter": "cap/%d/+" % i,
            "expectedPublisherGroups": [i],
            "persistentSessionInfo": {"clientType": "DEVICE"},
            "clientIdPrefix": None,
        }
        for i in range(1, GROUPS + 1)
    ]
    scenario = {
        "publisherGroups": publisher_groups,
        "subscriberGroups": subscriber_groups,
        "dummyClients": 0,
        "secondsToRun": 600,
        "additionalSecondsToWait": 30,
        "maxMsgsPerPublisherPerSecond": 1.1,
        "publisherQosValue": 1,
        "subscriberQosValue": 1,
        "minPayloadSize": 256,
        "maxConcurrentOperations": 1000,
        "telemetryKeys": [],
    }
    print(json.dumps(scenario, indent=2))


if __name__ == "__main__":
    main()
