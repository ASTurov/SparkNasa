#!/usr/bin/env bash

spark-submit \
    --class SparkStart \
    --master yarn \
    --deploy-mode client \
    --executor-memory 1G \
    --num-executors 3 \
    app.jar hadoop-master /nasa/Aug
