#!/bin/bash

sudo CEPH_DEV=1 ./bin/ceph osd pool create rbdtestpool 100
sudo CEPH_DEV=1 ./bin/ceph osd pool application enable rbdtestpool cephfs
# size: # of replicas
sudo CEPH_DEV=1 ./bin/ceph osd pool set rbdtestpool size 1
# min_size: # of replicas to be written before sending an ACK to a client 
sudo CEPH_DEV=1 ./bin/ceph osd pool set rbdtestpool min_size 1 

sudo ./bin/rados bench -p rbdtestpool -b 4096 --max-objects 100000 --run-name 1 -t 64 3600 write --no-cleanup
echo "sudo ./bin/rados bench -p rbd --max-objects 100000 --run-name 1 -t 64 30 rand"
echo "sudo ./bin/rados bench -p rbd --max-objects 100000 --run-name 1 -t 64 30 seq"

