#!/bin/bash

export PUBSUB_EMULATOR_HOST=localhost:8933

PUBSUB_PROJECT_ID=beam-demo
python ../pubsub-emulator/subscriber.py ${PUBSUB_PROJECT_ID} receive $1 
