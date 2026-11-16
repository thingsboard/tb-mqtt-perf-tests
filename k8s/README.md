To configure broker tests:

```
./k8s-deploy-broker-tests.sh
```

To delete broker tests:

```
./k8s-delete-broker-tests.sh
```

To run tests:

```
./start-test.sh
```

**Note:** you can change test run configuration in `test_run_config.json` file inside
the [broker-tests-publishers-config.yml](broker-tests-publishers-config.yml) config
or [broker-tests-subscribers-config.yml](broker-tests-subscribers-config.yml).

* publisherGroups - list of configured publisher groups
  * id - identifier of the group
  * publishers - number of publishers in the group
  * topicPrefix - topic prefix to which publishers from the group will send messages
  * clientIdPrefix - client id prefix for clients in the group
* subscriberGroups - list of configured subscriber groups
  * id - identifier of the group
  * subscribers - number of subscribers in the group
  * topicFilter - topic filter to which subscribers from the group will subscribe
  * expectedPublisherGroups - expected publisher groups' messages to receive
  * persistentSessionInfo - info about the client (contains `clientType` with values `APPLICATION`/`DEVICE`)
  * clientIdPrefix - client id prefix for clients in the group
* dummyClients - number of dummy clients that will be only connected to the broker
* secondsToRun - seconds to run the test (publishing of messages)
* additionalSecondsToWait - seconds to wait additionally after publishing is finished
* maxMsgsPerPublisherPerSecond - max number of messages sent per publisher per second (decimals are accepted, e.g. 1.1)
* publisherQosValue - publish QoS
* subscriberQosValue - subscribe QoS
* minPayloadSize - min payload size that will be generated
* maxConcurrentOperations - max concurrent operations (connects or subscribes) per broker cluster per time
* telemetryKeys - list of telemetry keys used to generate a publishing message