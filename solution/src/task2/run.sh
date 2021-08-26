#!/bin/bash
cd ../../..
docker build -t task2 -f ./solution/src/task2/Dockerfile  .
docker run task2
