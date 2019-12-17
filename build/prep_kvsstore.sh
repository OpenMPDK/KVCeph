devicename=$(grep CEPH_OSD0_DEVS ./kvceph-conf/store_env.conf | cut -d '(' -f 2 | cut -d ')' -f 1)
echo "DEVICE: $devicename"
echo "Format device $devicename"
sudo nvme format $devicename -s0 -n1
sudo ./kvlog $devicename
rm -rf ftrace.txt
status=$?
[ $status -eq 0 ] && echo "format was successful" || exit 1
