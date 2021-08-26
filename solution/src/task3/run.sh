#!/bin/bash
cd ../../..
docker build -t task3 -f ./solution/src/task3/Dockerfile  .
docker run task3
