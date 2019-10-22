#!/bin/bash
echo "Stop any running ceph processes"
sudo ../src/stop.sh

echo "Remove previous local directories"
sudo rm -rf out
sudo rm -rf dev

echo "## VSTART"
echo "sudo CEPH_DEV=1 MON=1 MDS=1 MGR=1 RGW=1 OSD=1 ../src/vstart.sh -n -X --bluestore"
sudo CEPH_DEV=1 MON=1 MDS=1 MGR=1 RGW=1 OSD=1 ../src/vstart.sh -n -X --bluestore
