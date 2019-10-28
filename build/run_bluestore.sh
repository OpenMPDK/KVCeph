#!/bin/bash

echo "Deploy Mode SingleNode for Bluestore"
device=$1

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

