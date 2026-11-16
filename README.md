# performance-tests

ThingsBoard MQTT Broker performance tests.

Project that is able to stress test ThingsBoard MQTT broker with a huge number of MQTT messages published simultaneously
from different clients.

## Prerequisites

- [Install Docker CE](https://docs.docker.com/engine/installation/)
- [Install kubectl](https://kubernetes.io/docs/tasks/tools/)

## Running

See [k8s](/k8s/README.md) README for more details.

Main configuration parameters:

* HTTP_BIND_PORT - port of the server. Default: 8088
* MQTT_HOST - URL of the ThingsBoard MQTT broker. Default: localhost
* MQTT_PORT - port of the ThingsBoard MQTT broker. Default: 1883
* TB_URI - URI of the ThingsBoard MQTT broker. Default: http://localhost:8083
* TB_ADMIN_USERNAME - ThingsBoard MQTT broker username. Default: sysadmin@thingsboard.org
* TB_ADMIN_PASSWORD - ThingsBoard MQTT broker password. Default: sysadmin
* ORCHESTRATION_NODE - if run service as orchestrator(node that orchestrate the cluster of runners) or runner(node that
  creates clients and publish messages, etc.). Values: true/false
* TEST_RUN_TEST_ORCHESTRATOR_URL - orchestrator node URL
* TEST_RUN_TEST_NODE_URL - runner node URL
* TEST_RUN_CONFIGURATION_FILE - path to performance test configuration file
* TEST_RUN_SEQUENTIAL_NUMBER - sequential number of runner nodes
* TEST_RUN_PARALLEL_TESTS_COUNT - how many test nodes (runners) will be launched
* TEST_RUN_TOTAL_TESTS_COUNT - sum of test nodes when subscribers and publishers are separated
* TEST_RUN_MAX_CLUSTER_WAIT_TIME_SECONDS - max time in seconds to wait in runners and orchestrator for others to be
  ready
* TEST_RUN_CLUSTER_PROCESS_WAIT_TIME_SECONDS - max time in seconds to wait for clients to connect/subscribe to topics
* TEST_RUN_CLEAR_PERSISTED_SESSIONS_WAIT_TIME_SECONDS - max time in seconds to wait to clear persisted sessions
* TEST_RUN_WAIT_TIME_MS_AFTER_CLIENTS_DISCONNECT - time in milliseconds to sleep after the clients started disconnecting
* TEST_RUN_MAX_TOTAL_CLIENTS_PER_ITERATION - max number out of all publishers that will be used per publishing iteration
* TEST_RUN_RECEIVE_THREADS - number of threads that process received messages (messages of one subscriber are always
  processed by the same thread, in order). Default: 0, which means the number of available processors
