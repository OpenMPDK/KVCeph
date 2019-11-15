#!/bin/bash

device=$(grep CEPH_OSD0_DEVS ./kvceph-conf/bluestore_env.conf | cut -d '(' -f 2 | cut -d ')' -f 1)
echo "DEVICE: $devicename"

echo "Deploy Mode SingleNode for Bluestore"

if [ -z "$device" ]
then
   echo "No device specified. Selecting default from configuration file"
   grep nvme kvceph-conf/bluestore_env.conf
   ./setup_clusters_singlenode.sh bluestore
else
   echo "Selected device: $device"
   echo "Checking for device in host"
   sudo nvme list | grep $device
   ./setup_clusters_singlenode.sh bluestore $device
fi

