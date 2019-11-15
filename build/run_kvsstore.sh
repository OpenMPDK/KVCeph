#!/bin/bash

devicename=$(grep CEPH_OSD0_DEVS ./kvceph-conf/store_env.conf | cut -d '(' -f 2 | cut -d ')' -f 1)
echo "DEVICE: $devicename"


echo "Stop any running ceph processes"
sudo ../src/stop.sh

echo "Remove existing local directories"
sudo rm -rf keyring
sudo rm -rf out
sudo rm -rf dev


echo "Format device $devicename"
sudo nvme format $devicename -s0 -n1
echo "## VSTART"
echo "sudo CEPH_DEV=1 MON=1 MDS=1 MGR=1 RGW=1 OSD=1 ../src/vstart_kvs.sh -n -X --kvsstore"
sudo CEPH_DEV=1 MON=1 MDS=1 MGR=1 RGW=1 OSD=1 ../src/vstart_kvs.sh -n -X --kvsstore --kvsstore-dev $devicename
