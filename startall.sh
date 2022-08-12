#!/bin/bash

./bin/zkServer.sh --config ~/work/zookeeper-repos/zk_configs/zoo1 start
./bin/zkServer.sh --config ~/work/zookeeper-repos/zk_configs/zoo2 start
./bin/zkServer.sh --config ~/work/zookeeper-repos/zk_configs/zoo3 start
