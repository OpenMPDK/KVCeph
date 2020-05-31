#!/bin/bash

sudo CEPH_DEV=1 ./bin/ceph osd pool create rbd 100
sudo CEPH_DEV=1 ./bin/ceph osd pool application enable rbd cephfs
# size: # of replicas
sudo CEPH_DEV=1 ./bin/ceph osd pool set rbd size 1
# min_size: # of replicas to be written before sending an ACK to a client 
sudo CEPH_DEV=1 ./bin/ceph osd pool set rbd min_size 1 

sudo ./bin/rados bench -p rbd -b 4096 --max-objects 1000000 --run-name 1 -t 64 3600 write --no-cleanup
echo "sudo ./bin/rados bench -p rbd --max-objects 100000 --run-name 1 -t 64 30 rand"
echo "sudo ./bin/rados bench -p rbd --max-objects 100000 --run-name 1 -t 64 30 seq"

