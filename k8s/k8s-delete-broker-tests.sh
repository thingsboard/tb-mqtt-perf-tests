#!/bin/bash
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

set -e

kubectl config set-context $(kubectl config current-context) --namespace=thingsboard-mqtt-broker

kubectl delete -f broker-tests-publishers-config.yml
kubectl delete -f broker-tests-subscribers-config.yml
kubectl delete -f broker-tests-orch-config.yml

kubectl delete -f broker-tests-publishers.yml
kubectl delete -f broker-tests-subscribers.yml
kubectl delete -f broker-tests-orch.yml

kubectl delete -f tb-kafka-ui-kowl.yml
