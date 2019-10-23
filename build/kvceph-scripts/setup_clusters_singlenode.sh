#!/bin/bash

# October 22 2019
#  ./setup_clusters_singlenode.sh arg1
# Script arguments:
#  arg1= backendtype (bluestore || kvsstore)

if [ $# != 1 ]; then
   echo "Distribute and launch KVceph"
   echo "Usage: $0 [kvsstore|bluestore]"
   exit
fi 

OS=$1
if [[  $OS != "bluestore" ]] && [[ $OS != "kvsstore" ]]; then
        echo "$OS not supported"
        exit
fi

CEPH_NUM_NODES=1
CEPH_NUM_OSDS_PER_NODE=1


function deploy_conf() {
        from=$1
        
        for (( i=0; i < $CEPH_NUM_NODES; i++)); do
                nodeip=${CEPH_NODE_IPS}
                homedir=$HOMEDIR1$CEPH_DST_DEPLOY_DIR_SHORT

                echo "[frombuild] deploying ceph.conf to $nodeip:$homedir"
                scp -q $from $nodeip:$homedir/ceph.conf
        done
}


function deploy_dir() {
        from=$1
        to=$2
        if [ -z $to ]; then
                echo "err: missing destination, usage: deploy_dir from to"
                exit
        fi

        for (( i=0; i < $CEPH_NUM_NODES; i++)); do
                nodeip=${CEPH_NODE_IPS}
                # delete the DST directory
                echo "[$nodeip] sudo rm -rf $to"
                sudo rm -rf $to
                sleep 2
                echo "[$nodeip] mkdir -p $to"
                mkdir -p $to
                
                # copy the dir to user's home directory specified in the configuration file
                homedir=$HOMEDIR1
                echo "[frombuild $from] deploying ceph-runtime to $nodeip:$homedir"
                scp -pq -r $from $nodeip:$homedir

        done
        
}

function umount_osds() {

         for (( i=0; i < $CEPH_NUM_NODES; i++)); do
                nodeip=${CEPH_NODE_IPS}
                echo "[$nodeip] unmounting devices"
                for (( devid=0; devid < $CEPH_NUM_OSDS_PER_NODE; devid++)); do
                        eval DEV=( \${CEPH_OSD${i}_DEVS[$devid]} )
                        sudo umount -f ${DEV}p1 || echo ''
                done
        done
}

function format_devices() {
 
      for (( i=0; i < $CEPH_NUM_NODES; i++)); do
                nodeip=${CEPH_NODE_IPS}
                echo "[$nodeip] formatting devices"
                for (( devid=0; devid < $CEPH_NUM_OSDS_PER_NODE; devid++)); do
                      eval DEV=( \${CEPH_OSD${i}_DEVS[$devid]} )
                      echo "$nodeip sudo nvme format $DEV -s1 -n1"
                      #sudo nvme format $DEV -s1 -n1
                      sudo nvme format $DEV -s0
                done

      done
}

function setup_remote_deploy_dir() {
        to=$1
        if [ -z $to ]; then
                echo "err: missing destination, usage: setup_remote_deploy_dir to"
                exit
        fi
        
        osdid_n=0
        for (( i=0; i < $CEPH_NUM_NODES; i++)); do
                nodeip=${CEPH_NODE_IPS}

                echo "[$nodeip] Setting up deploy directories in $HOMEDIR1"
                dest_to=$HOMEDIR1$CEPH_DST_DEPLOY_DIR_SHORT
                echo "[$nodeip] Directory path set to $dest_to" 

                osdid=0

                echo "[$nodeip] unmounting devices"
                for (( devid=0; devid < $CEPH_MAX_OSDS_PER_NODE; devid++)); do
                        eval DEV=( \${CEPH_OSD${i}_DEVS[$devid]} )
                        mountpoint -q ${dest_to}/osd.$osdid/data && sudo fuser -c -k ${dest_to}/osd.$osdid/data || echo ''
                        mountpoint -q ${dest_to}/osd.$osdid/data && sudo umount -f ${dest_to}/osd.$osdid/data || echo ''
                        sudo rm -f /dev/disk/by-partlabel/osd-${osdid}-*
                        osdid=$(($osdid + 1))
                done

                # delete the DST directory
                echo "[$nodeip] Remove directory $dest_to"
                sudo rm -rf $dest_to
                echo "[$nodeip] mkdir -p $dest_to/mon"
                mkdir -p $dest_to/mon
                echo "[$nodeip] mkdir -p $dest_to/out"
                mkdir -p $dest_to/out
                mkdir -p $dest_to/run
               
                for (( devid=0; devid < $CEPH_NUM_OSDS_PER_NODE; devid++)); do
                        eval devpath=( \${CEPH_OSD${i}_DEVS[$devid]} )
                        mkdir -p $dest_to/osd.$osdid_n/data
                        osdid_n=$(($osdid_n + 1))
                done
        done
}

function setup_kvs_osds() {
        output=$1

        osdid=0
        for (( i=0; i < $CEPH_NUM_NODES; i++)); do
                hname=${CEPH_NODE_HOSTNAMES}
                nodeip=${CEPH_NODE_IPS}

                dest_dir=$HOMEDIR1$CEPH_DST_DEPLOY_DIR_SHORT
                echo "[$nodeip] Directory path set to $dest_dir"
                dest_runtime_dir=$HOMEDIR1$CEPH_DST_RUNTIME_DIR_SHORT
                echo "[$nodeip] Dest runtime dir = $dest_runtime_dir"

                for (( devid=0; devid < $CEPH_NUM_OSDS_PER_NODE; devid++)); do
                        eval DEV=( \${CEPH_OSD${i}_DEVS[$devid]} )

                        OSD_SECRET=$(export LD_LIBRARY_PATH=${dest_runtime_dir}${CEPH_DST_RUNTIME_DIR_LIB}:/usr/local/lib/ceph && sudo ldconfig && cd $dest_dir && $dest_runtime_dir/bin/ceph-authtool --gen-print-key)
                        echo "[$nodeip] $OSD_SECRET"
                        eval "CEPH_OSD${i}_SECRETS[$devid]=$OSD_SECRET" 
                        eval_ret_code=$?
                        echo "[osd.$osdid]
        host = $hname
        public addr = $nodeip
        cluster addr = $nodeip
        key = $OSD_SECRET
        log file = $dest_dir/osd.$osdid/osd.log
        osd data = $dest_dir/osd.$osdid/data
        keyring = $dest_dir/ceph.mon.keyring
        kvsstore_dev_path = $DEV 
        run dir = $dest_dir/run" >> $output
                        osdid=$(($osdid + 1))
              done

        done

}


function setup_bs_osds() {
        output=$1
       
        osdid=0
        for (( i=0; i < $CEPH_NUM_NODES; i++)); do
                hname=${CEPH_NODE_HOSTNAMES}
                nodeip=${CEPH_NODE_IPS}

                dest_dir=$HOMEDIR1$CEPH_DST_DEPLOY_DIR_SHORT
                echo "[$nodeip] Directory path set to $dest_dir"
                dest_runtime_dir=$HOMEDIR1$CEPH_DST_RUNTIME_DIR_SHORT
                echo "[$nodeip] Dest runtime dir = $dest_runtime_dir"

                #create partitions 
                for (( devid=0; devid < $CEPH_NUM_OSDS_PER_NODE; devid++)); do
                        eval DEV=( \${CEPH_OSD${i}_DEVS[$devid]} )


                        echo "[$nodeip] partitioning $DEV"

                        sudo parted -s -a optimal $DEV mklabel gpt || failed 'mklabel $DEV'
                        sudo parted -s -a optimal $DEV mkpart osd-${osdid}-data  0G 10G
                        sudo parted -s -a optimal $DEV mkpart osd-${osdid}-db    10G 20G
                        sudo parted -s -a optimal $DEV mkpart osd-${osdid}-wal   20G 30G
                        sudo parted -s -a optimal $DEV mkpart osd-${osdid}-block 30G 100%
                        ls -lhtr /dev/disk/by-partlabel/
                        sleep 1
                        sudo mkfs.xfs -f /dev/disk/by-partlabel/osd-${osdid}-data
                        sudo mount /dev/disk/by-partlabel/osd-${osdid}-data $dest_dir/osd.$osdid/data


                        OSD_SECRET=$(export LD_LIBRARY_PATH=${dest_runtime_dir}${CEPH_DST_RUNTIME_DIR_LIB}:/usr/local/lib/ceph && sudo ldconfig && cd $dest_dir && $dest_runtime_dir/bin/ceph-authtool --gen-print-key)
                        echo "[$nodeip] $OSD_SECRET"
                        echo "CEPH_OSD${i}_SECRETS[$devid]"
                        eval "CEPH_OSD${i}_SECRETS[$devid]=$OSD_SECRET" 
                        eval_ret_code=$?
                        echo "$eval_ret_code" 
                        echo "after eval secrets appending to $output"
                        echo "[osd.$osdid]
        host = $hname
        public addr = $nodeip
        cluster addr = $nodeip
        key = $OSD_SECRET
        log file = $dest_dir/osd.$osdid/osd.log
        osd data = $dest_dir/osd.$osdid/data
        rocksdb_log = $dest_dir/out/rocksdb.log
        keyring = $dest_dir/ceph.mon.keyring
        run dir = $dest_dir/run
        bluestore block path = /dev/disk/by-partlabel/osd-${osdid}-block
        bluestore block db path = /dev/disk/by-partlabel/osd-${osdid}-db
        bluestore block wal path = /dev/disk/by-partlabel/osd-${osdid}-wal" >> $output
                        osdid=$(($osdid + 1))
                done

      done
      
}

function start_monitor() {
        CEPH_BIN=${CEPH_DST_RUNTIME_DIR}/bin
        CEPH_AUTHTOOL=${CEPH_BIN}/ceph-authtool
        CEPH_MONMAPTOOL=${CEPH_BIN}/monmaptool

        echo "[${CEPH_MON_IP}] starting a monitor"
        cd ${CEPH_DST_DEPLOY_DIR} && $CEPH_AUTHTOOL --create-keyring ./ceph.mon.keyring --gen-key -n mon. --cap mon 'allow *'
        cd ${CEPH_DST_DEPLOY_DIR} && $CEPH_AUTHTOOL --create-keyring ./ceph.client.admin.keyring --gen-key -n client.admin --cap mon 'allow *' --cap osd 'allow *' --cap mds 'allow *' --cap mgr 'allow *'
        cd ${CEPH_DST_DEPLOY_DIR} && $CEPH_AUTHTOOL --create-keyring ./ceph.keyring --gen-key -n client.bootstrap-osd --cap mon 'profile bootstrap-osd'
        cd ${CEPH_DST_DEPLOY_DIR} && $CEPH_AUTHTOOL ./ceph.mon.keyring --import-keyring ./ceph.client.admin.keyring
        cd ${CEPH_DST_DEPLOY_DIR} && $CEPH_AUTHTOOL ./ceph.mon.keyring --import-keyring ./ceph.keyring
        cd ${CEPH_DST_DEPLOY_DIR} && $CEPH_MONMAPTOOL --create --add $CEPH_MON_HOSTNAMES $CEPH_MON_IP --fsid $CEPH_MON_FSID ./monmap

        cd ${CEPH_DST_DEPLOY_DIR} && sudo ${CEPH_BIN}/ceph-mon --mkfs -i $CEPH_MON_HOSTNAMES --monmap ./monmap --keyring ./ceph.mon_keyring
        cd ${CEPH_DST_DEPLOY_DIR} && sudo ${CEPH_BIN}/ceph-mon -i $CEPH_MON_HOSTNAMES
        cd ${CEPH_DST_DEPLOY_DIR} && sudo ${CEPH_BIN}/ceph mon enable-msgr2 -c ./ceph.conf

        cd ${CEPH_DST_DEPLOY_DIR} && ${CEPH_BIN}/ceph -s

}

function start_manager() {
        CEPH_BIN=${CEPH_DST_RUNTIME_DIR}/bin
        echo "[${CEPH_MON_IP}] starting a ceph manager daemon"
        # Create an authentication key for the daemon
        cd ${CEPH_DST_DEPLOY_DIR} && $CEPH_BIN/ceph auth get-or-create mgr.sam mon 'allow profile mgr' osd 'allow *' mds 'allow *'

        #Start the ceph-mgr daemon
        cd ${CEPH_DST_DEPLOY_DIR} && $CEPH_BIN/ceph-mgr -i sam
}

function stop_ceph() {

   # double kill/decrease if needed
   for k in {1..2}
   do
        for (( i=0; i < $CEPH_MON_NODES; i++)); do
                nodeip=${CEPH_MON_IP}
                echo "[$nodeip] stop ceph services..."
                sudo killall -e ceph-mon
                sudo killall -e ceph-mgr
        done

        for (( i=0; i < $CEPH_NUM_NODES; i++)); do
                nodeip=${CEPH_NODE_IPS}
                echo "[$nodeip] stop ceph services..."
                sudo killall -e ceph-osd
        done
        sleep 2
   done
   
}

function start_osd2() {
        osdid=0
        for (( i=0; i < $CEPH_NUM_NODES; i++)); do
                nodeip=${CEPH_NODE_IPS}
                nodename=${CEPH_NODE_HOSTNAMES}

              
                CEPH_BIN=$HOMEDIR1$CEPH_DST_RUNTIME_DIR_SHORT/bin
                CEPH_DST_RUNTIME_DIR=$HOMEDIR1$CEPH_DST_RUNTIME_DIR_SHORT/
                CEPH_DST_DEPLOY_DIR=$HOMEDIR1$CEPH_DST_DEPLOY_DIR_SHORT
                
		remote_homedir=$HOMEDIR1
                
		echo "NODE: $nodeip"
                echo " - REMOTE_CEPHBIN: $CEPH_BIN"
                echo " - REMOTE_HOME: $remote_homedir"
                echo " - REMOTE_DEPLOY: $CEPH_DST_DEPLOY_DIR"

                for (( devid=0; devid < $CEPH_NUM_OSDS_PER_NODE; devid++)); do

                        eval DEV=( \${CEPH_OSD${i}_DEVS[$devid]} )
                        eval OSD_SECRET=( \${CEPH_OSD${i}_SECRETS[$devid]} )
                        local uuid=`uuidgen`
                        cephx_secret="{\"cephx_secret\": \"$OSD_SECRET\"}"

                        echo " - DEVICE: $DEV"
                        echo " - cephx_secret: $cephx_secret"
                        echo " - uuid: $uuid"

                        # create OSD
                        id=$(cd ${CEPH_DST_DEPLOY_DIR} && echo "$cephx_secret" | ${CEPH_BIN}/ceph osd new $uuid $osdid -i - -n client.bootstrap-osd -k ./ceph.keyring)

			echo ">> OSD $id is created"

                        export LD_LIBRARY_PATH=${CEPH_DST_RUNTIME_DIR}${CEPH_DST_RUNTIME_DIR_LIB}:/usr/local/lib/ceph && sudo ldconfig && cd ${CEPH_DST_DEPLOY_DIR} && ${CEPH_BIN}/ceph-authtool --create-keyring ${CEPH_DST_DEPLOY_DIR}/osd.$osdid/keyring --name osd.$osdid --add-key $OSD_SECRET
                        cd ${CEPH_DST_DEPLOY_DIR} && cat ./ceph.conf | grep objectstore
                        
			echo ">> OSD keyring is created"

                        if [[ $OS == "kvsstore" ]]; then
                          export LD_LIBRARY_PATH=${CEPH_DST_RUNTIME_DIR}${CEPH_DST_RUNTIME_DIR_LIB}:/usr/local/lib/ceph && sudo ldconfig && cd ${CEPH_DST_DEPLOY_DIR} && sudo LD_LIBRARY_PATH=${remote_homedir}/ceph-runtime/lib/ceph $CEPH_BIN/ceph-osd -i $osdid --mkfs --key $OSD_SECRET --osd-uuid $uuid -c ./ceph.conf
                        elif [[ $OS == "bluestore" ]]; then
                           
                          echo "cd ${CEPH_DST_DEPLOY_DIR} && sudo $CEPH_BIN/ceph-osd -i $osdid --mkfs --key $OSD_SECRET --osd-uuid $uuid -c ./ceph.conf"
                          cd ${CEPH_DST_DEPLOY_DIR} && sudo $CEPH_BIN/ceph-osd -i $osdid --mkfs --key $OSD_SECRET --osd-uuid $uuid -c ./ceph.conf
                        fi

                        # start OSD
                        echo "sudo $CEPH_BIN/ceph-osd -i $osdid -c ${CEPH_DST_DEPLOY_DIR}/ceph.conf"
                        sudo LD_LIBRARY_PATH=${remote_homedir}/ceph-runtime/lib/ceph $CEPH_BIN/ceph-osd -i $osdid -c ${CEPH_DST_DEPLOY_DIR}/ceph.conf
                        osdid=$(($osdid + 1))
                done
        done

}


# Main script Starts here

# Load configuration
if [[ $OS == "bluestore" ]]; then
 source setup_bluestore.conf.singlenode
elif  [[ $OS == "kvsstore" ]]; then
 source setup_kvsstore.conf.singlenode
fi

if [ ! -d "$CEPH_SRC_RUNTIME_DIR" ]; then
        echo "CEPH RUNTIME directory does not exist"
        echo "$CEPH_SRC_RUNTIME_DIR"
        exit
fi

stop_ceph

echo "deploy_dir $CEPH_SRC_RUNTIME_DIR $CEPH_DST_RUNTIME_DIR"

deploy_dir $CEPH_SRC_RUNTIME_DIR $CEPH_DST_RUNTIME_DIR
echo "Step 1 : deploy_dir completed"

setup_remote_deploy_dir $CEPH_DST_DEPLOY_DIR
echo "Step 2: setup_remote_deploy_dir completed"

if [[ $OS == "bluestore" ]]; then

        echo "Step 3.a: formatting devices"
	umount_osds
        sleep 5
        format_devices

	echo "create bluestore configuration"

        source $CEPH_ADMIN_DIR/bs_template.conf
        echo "$template_header" > $CEPH_ADMIN_DIR/temp.conf
        
        setup_bs_osds $CEPH_ADMIN_DIR/temp.conf 

        echo "Step 3.b: setup_bluestore_osds completed"

elif [[ $OS == "kvsstore" ]]; then

        echo "Step 3.a: formatting devices"
        format_devices

        source $CEPH_ADMIN_DIR/kvs_template.conf
        echo "$template_header" > $CEPH_ADMIN_DIR/temp.conf
        
        echo "Step 3.b: setup kvs osds"
        setup_kvs_osds $CEPH_ADMIN_DIR/temp.conf 
fi

deploy_conf $CEPH_ADMIN_DIR/temp.conf
echo "Step 4: deploy ceph conf completed"

start_monitor

echo "Step 5: monitor started"
start_manager

echo "Step 6: manager started"
start_osd2 

echo "Step 7: osds started"

ps -aux | grep ceph









