#!/bin/bash

devicenum=$(grep nvme ../src/vstart_kvs.sh | awk '{print $2}' | cut -d '"' -f 2)
echo "$devicenum specified in vstart_kvs.sh"


echo "Stop any running ceph processes"
sudo ../src/stop.sh

echo "Remove existing local directories"
sudo rm -rf keyring
sudo rm -rf out
sudo rm -rf dev


echo "Format device $devicenum"
sudo nvme format $devicenum -s0 -n1
echo "## VSTART"
echo "sudo CEPH_DEV=1 MON=1 MDS=1 MGR=1 RGW=1 OSD=1 ../src/vstart_kvs.sh -n -X --kvsstore"
sudo CEPH_DEV=1 MON=1 MDS=1 MGR=1 RGW=1 OSD=1 ../src/vstart_kvs.sh -n -X --kvsstore