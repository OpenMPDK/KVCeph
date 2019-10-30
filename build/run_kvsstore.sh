#!/bin/bash

devicenum=2 # Has to match the vstart_kvs.sh

echo "Stop any running ceph processes"
sudo ../src/stop.sh

echo "Remove previous local directories"
sudo rm -rf keyring
sudo rm -rf out
sudo rm -rf dev


echo "Format device $devicenum"
sudo nvme format /dev/nvme${devicenum}n1 -s0 -n1
echo "## VSTART"
echo "sudo CEPH_DEV=1 MON=1 MDS=0 MGR=1 RGW=0 OSD=1 ../src/vstart_kvs.sh -n -X --kvsstore"
sudo CEPH_DEV=1 MON=1 MDS=0 MGR=1 RGW=0 OSD=1 ../src/vstart_kvs.sh -n -X --kvsstore