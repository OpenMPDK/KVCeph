#!/bin/bash

# March 25 2019    
#  ./setup_singlenode.sh rg1 arg2 arg3 
# Script arguments:
#  arg1= number of Storage nodes , default =1 
#  arg2= kvsstore             
#  arg3= number of OSDs per node  

if [ $# != 3 ]; then
   echo "Distribute and launch ceph OSDs"
   echo "Usage: $0 [number of nodes 1] [kvsstore|bluestore] [number of osd per node 1~N]"
   exit
fi 

OS=$2
if [[ $OS != "kvsstore" ]]; then
        echo "$OS not supported"
        exit
fi

CEPH_NUM_NODES=$1
if [[ $CEPH_NUM_NODES =~ ^[1]+$ ]]; then
       echo "Deploy OSD to $CEPH_NUM_NODES node(s)"
else
       echo "Number of Nodes=$CEPH_NUM_NODES, Expected integer 1"
       exit
fi

CEPH_NUM_OSDS_PER_NODE=$3
if [[ $CEPH_NUM_OSDS_PER_NODE =~ ^[1-9]+$ ]]; then
       echo "Deploy $CEPH_NUM_OSDS_PER_NODE OSD per node"
       echo "Total cluster OSDs = $(($CEPH_NUM_OSDS_PER_NODE*$CEPH_NUM_NODES))"
else
       echo "Number of OSDs per Node=$CEPH_NUM_OSDS_PER_NODE, Expected integer between 1-9"
       exit
fi



function deploy_conf() {
        from=$1
        
        for (( i=0; i < $CEPH_NUM_NODES; i++)); do
                nodeip=${CEPH_NODE_IPS[$i]}
                homedir=$CEPH_DST_DEPLOY_DIR

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
                nodeip=${CEPH_NODE_IPS[$i]}
                # delete the DST directory
                sudo rm -rf $to
                sleep 2
                mkdir -p $to

                # copy the dir to user's home directory
                echo "[frombuild $from] deploying ceph-runtime to $nodeip:$CEPH_DST_RUNTIME_DIR"
                scp -pq -r $from $nodeip:$CEPH_DST_RUNTIME_DIR
                cd $CEPH_DST_RUNTIME_DIR && mv ceph-runtime/* .

        done
}

function umount_osds() {

         for (( i=0; i < $CEPH_NUM_NODES; i++)); do
                nodeip=${CEPH_NODE_IPS[$i]}
                echo "[$nodeip] unmounting devices"
                for (( devid=0; devid < $CEPH_NUM_OSDS_PER_NODE; devid++)); do
                        eval DEV=( \${CEPH_OSD${i}_DEVS[$devid]} )
                        sudo umount -f ${DEV}p1 || echo ''
                done
        done
}

function format_devices() {
 
      for (( i=0; i < $CEPH_NUM_NODES; i++)); do
                nodeip=${CEPH_NODE_IPS[$i]}
                echo "[$nodeip] formatting devices"
                for (( devid=0; devid < $CEPH_NUM_OSDS_PER_NODE; devid++)); do
                      eval DEV=( \${CEPH_OSD${i}_DEVS[$devid]} )
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
                nodeip=${CEPH_NODE_IPS[$i]}

                dest_to=$CEPH_DST_DEPLOY_DIR
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
                hname=${CEPH_NODE_HOSTNAMES[$i]}
                nodeip=${CEPH_NODE_IPS[$i]}

                dest_dir=$CEPH_DST_DEPLOY_DIR
                echo "[$nodeip] Directory path set to $dest_dir"
                dest_runtime_dir=$CEPH_DST_RUNTIME_DIR
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


function start_monitor() {
        CEPH_BIN=${CEPH_DST_RUNTIME_DIR}/bin
        CEPH_AUTHTOOL=${CEPH_BIN}/ceph-authtool
        CEPH_MONMAPTOOL=${CEPH_BIN}/monmaptool

        echo "[${CEPH_MON_IP}] starting a monitor"
        cd ${CEPH_DST_DEPLOY_DIR} && $CEPH_AUTHTOOL --create-keyring ./ceph.mon.keyring --gen-key -n mon. --cap mon 'allow *'
        cd ${CEPH_DST_DEPLOY_DIR} && $CEPH_AUTHTOOL --create-keyring ./ceph.client.admin.keyring --gen-key -n client.admin --set-uid=0 --cap mon 'allow *' --cap osd 'allow *' --cap mds 'allow *' --cap mgr 'allow *'
        cd ${CEPH_DST_DEPLOY_DIR} && $CEPH_AUTHTOOL --create-keyring ./ceph.keyring --gen-key -n client.bootstrap-osd --cap mon 'profile bootstrap-osd'
        cd ${CEPH_DST_DEPLOY_DIR} && $CEPH_AUTHTOOL ./ceph.mon.keyring --import-keyring ./ceph.client.admin.keyring
        cd ${CEPH_DST_DEPLOY_DIR} && $CEPH_AUTHTOOL ./ceph.mon.keyring --import-keyring ./ceph.keyring
        cd ${CEPH_DST_DEPLOY_DIR} && $CEPH_MONMAPTOOL --create --add $CEPH_MON_HOSTNAMES $CEPH_MON_IP --fsid $CEPH_MON_FSID ./monmap

        cd ${CEPH_DST_DEPLOY_DIR} && sudo ${CEPH_BIN}/ceph-mon --mkfs -i $CEPH_MON_HOSTNAMES --monmap ./monmap --keyring ./ceph.mon_keyring
        cd ${CEPH_DST_DEPLOY_DIR} && sudo ${CEPH_BIN}/ceph-mon -i $CEPH_MON_HOSTNAMES
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
                nodeip=${CEPH_MON_IP[$i]}
                echo "[$nodeip] stop ceph services..."
                sudo killall -e ceph-mon
                sudo killall -e ceph-mgr
        done

        for (( i=0; i < $CEPH_NUM_NODES; i++)); do
                nodeip=${CEPH_NODE_IPS[$i]}
                echo "[$nodeip] stop ceph services..."
                sudo killall -e ceph-osd
        done
        sleep 2
   done
}

function start_osd2() {
        osdid=0
        for (( i=0; i < $CEPH_NUM_NODES; i++)); do
                nodeip=${CEPH_NODE_IPS[$i]}
                nodename=${CEPH_NODE_HOSTNAMES[$i]}

                #deployto=$(run_remote_cmd $nodeip "pwd")
                CEPH_BIN=${CEPH_DST_RUNTIME_DIR}/bin
                CEPH_DST_RUNTIME_DIR=${CEPH_DST_RUNTIME_DIR}
                CEPH_DST_DEPLOY_DIR=${CEPH_DST_DEPLOY_DIR}
                
		remote_homedir=$HOME
                
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
                        #run_remote_cmd ${nodeip} "cd ${CEPH_DST_DEPLOY_DIR} && sudo chown -R $USER:$USER ${CEPH_DST_DEPLOY_DIR}/osd.$osdid"

                        # start OSD
                        echo "sudo $CEPH_BIN/ceph-osd -i $osdid -c ${CEPH_DST_DEPLOY_DIR}/ceph.conf"
                        sudo LD_LIBRARY_PATH=${remote_homedir}/ceph-runtime/lib/ceph $CEPH_BIN/ceph-osd -i $osdid -c ${CEPH_DST_DEPLOY_DIR}/ceph.conf
                        osdid=$(($osdid + 1))
                done
        done

}

function create_pools() {
       index=$1

       nodeip=${CEPH_NODE_IPS[$index]}
       nodename=${CEPH_NODE_HOSTNAMES[$index]}

       CEPH_BIN=$CEPH_DST_RUNTIME_DIR/bin
       echo "$CEPH_BIN"
       echo "${CEPH_DST_RUNTIME_DIR}"
       echo "$CEPH_DST_DEPLOY_DIR"

       #create pool
       echo "cd ${CEPH_DST_DEPLOY_DIR} && sudo $CEPH_BIN/ceph osd pool create mypool ${PG} ${PG}  -c ./ceph.conf"
       cd ${CEPH_DST_DEPLOY_DIR} && sudo $CEPH_BIN/ceph osd pool create mypool $PG $PG -c ${CEPH_DST_DEPLOY_DIR}/ceph.conf

       # list pools - verification step - optional
       echo "cd ${CEPH_DST_DEPLOY_DIR} && sudo $CEPH_BIN/ceph osd lspools -c ./ceph.conf"
       cd ${CEPH_DST_DEPLOY_DIR} && sudo $CEPH_BIN/ceph osd lspools -c ${CEPH_DST_DEPLOY_DIR}/ceph.conf

       # enable pool
       echo "cd ${CEPH_DST_DEPLOY_DIR} && sudo $CEPH_BIN/ceph osd pool application enable mypool cephfs -c ./ceph.conf"
       cd ${CEPH_DST_DEPLOY_DIR} && sudo $CEPH_BIN/ceph osd pool application enable mypool cephfs -c ${CEPH_DST_DEPLOY_DIR}/ceph.conf

       # set the number of replicas to 1 (experimental)
       echo "Setting number of replicas"
       cd ${CEPH_DST_DEPLOY_DIR} && ${CEPH_DST_RUNTIME_DIR}/bin/ceph osd pool set rbd size 1
       cd ${CEPH_DST_DEPLOY_DIR} && ${CEPH_DST_RUNTIME_DIR}/bin/ceph osd pool set rbd min_size 1

}

# Main script

# Load configuration
if  [[ $OS == "kvsstore" ]]; then
 source setup_kvsstore.conf
else
 echo "Invalid backend type"
 exit
fi

if [ ! -d "$CEPH_SRC_RUNTIME_DIR" ]; then
        echo "CEPH RUNTIME directory does not exist"
        echo "$CEPH_SRC_RUNTIME_DIR"
        exit
fi

stop_ceph

deploy_dir $CEPH_SRC_RUNTIME_DIR $CEPH_DST_RUNTIME_DIR
echo "Step 1 : deploy_dir completed"

setup_remote_deploy_dir $CEPH_DST_DEPLOY_DIR
echo "Step 2: setup_remote_deploy_dir completed"


echo "Step 3.a: formatting devices"
format_devices

source $CEPH_ADMIN_DIR/kvs_template.conf
echo "$template_header" > $CEPH_ADMIN_DIR/temp.conf
        
echo "Step 3.b: setup kvs osds"
setup_kvs_osds $CEPH_ADMIN_DIR/temp.conf 

deploy_conf $CEPH_ADMIN_DIR/temp.conf
echo "Step 4: deploy ceph conf completed"

start_monitor

echo "Step 5: monitor started"
start_manager

echo "Step 6: manager started"
create_pools 0

echo "Step 7: osd pools created"
start_osd2 

echo "Step 8: osds started"

ps -aux | grep ceph

sleep 1

# Run rados bench 

echo "Do you want to run rados bench WRITE?"
read -r -p  "Are you sure? [y/N] " response
if [[ "$response" =~ ^([yY][eE][sS]|[yY])+$ ]]
then
sudo LD_LIBRARY_PATH=${CEPH_DST_RUNTIME_DIR}/lib/  ${CEPH_DST_RUNTIME_DIR}/bin/rados bench -p mypool -b 4096 -t 128 30 write  --no-cleanup -c ./ceph.conf
else
 echo "Cluster is still running. Launch rados bench from the command line"
fi
