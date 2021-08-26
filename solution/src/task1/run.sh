#!/bin/bash
cd ../../..
docker build -t task1 -f ./solution/src/task1/Dockerfile  .
docker run task1
