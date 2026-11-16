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
Its format is described in the [main README](../README.md#scenario-file-format).
