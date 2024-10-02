#!/bin/bash

export PUBSUB_EMULATOR_HOST=localhost:8933

PUBSUB_PROJECT_ID=beam-demo
python ../pubsub-emulator/publisher.py ${PUBSUB_PROJECT_ID} publish input_topic
