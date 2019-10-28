#!/bin/bash
ulimit -n 1000000
use_gdb=$1

device_num=0       # Device in vstart.sh should match

echo "Stop any running ceph processes"
sudo ../src/stop.sh

echo "Remove local directories"
sudo rm -rf out
sudo rm -rf dev

echo "Formatting device nvme${device_num}"
sudo nvme format /dev/nvme${device_num}n1 -s0 -n1

if [ -z $use_gdb ]; then

        echo "sudo CEPH_DEV=1 MON=1 MDS=1 MGR=1 RGW=1 OSD=1 ../src/vstart.sh -n -X --kvsstore $use_gdb"
        sudo CEPH_DEV=1 MON=1 MDS=1 MGR=1 RGW=1 OSD=1 ../src/vstart.sh -n -X --kvsstore $use_gdb
else
        echo "sudo CEPH_DEV=1 MON=1 MDS=1 MGR=1 RGW=1 OSD=1 ../src/vstart_dbg.sh -n -X --kvsstore --gdb"
        sudo CEPH_DEV=1 MON=1 MDS=1 MGR=1 RGW=1 OSD=1 ../src/vstart_dbg.sh -n -X --kvsstore --gdb
fi

