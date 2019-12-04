#!/usr/bin/env bash
# -*- mode:sh; tab-width:4; sh-basic-offset:4; indent-tabs-mode:nil -*-
# vim: softtabstop=4 shiftwidth=4 expandtab

# abort on failure
set -e

if [ -n "$VSTART_DEST" ]; then
    SRC_PATH=`dirname $0`
    SRC_PATH=`(cd $SRC_PATH; pwd)`

    CEPH_DIR=$SRC_PATH
    CEPH_BIN=${PWD}/bin
    CEPH_LIB=${PWD}/lib

    CEPH_CONF_PATH=$VSTART_DEST
    CEPH_DEV_DIR=$VSTART_DEST/dev
    CEPH_OUT_DIR=$VSTART_DEST/out
    CEPH_ASOK_DIR=$VSTART_DEST/out
fi

get_cmake_variable() {
    local variable=$1
    grep "$variable" CMakeCache.txt | cut -d "=" -f 2
}

# for running out of the CMake build directory
if [ -e CMakeCache.txt ]; then
    # Out of tree build, learn source location from CMakeCache.txt
    CEPH_ROOT=$(get_cmake_variable ceph_SOURCE_DIR)
    CEPH_BUILD_DIR=`pwd`
    [ -z "$MGR_PYTHON_PATH" ] && MGR_PYTHON_PATH=$CEPH_ROOT/src/pybind/mgr
    CEPH_MGR_PY_VERSION_MAJOR=$(get_cmake_variable MGR_PYTHON_VERSION | cut -d '.' -f 1)
    if [ -n "$CEPH_MGR_PY_VERSION_MAJOR" ]; then
        CEPH_PY_VERSION_MAJOR=${CEPH_MGR_PY_VERSION_MAJOR}
    else
        if [ $(get_cmake_variable WITH_PYTHON2) = ON ]; then
            CEPH_PY_VERSION_MAJOR=2
        else
            CEPH_PY_VERSION_MAJOR=3
        fi
    fi
fi

# use CEPH_BUILD_ROOT to vstart from a 'make install' 
if [ -n "$CEPH_BUILD_ROOT" ]; then
    [ -z "$CEPH_BIN" ] && CEPH_BIN=$CEPH_BUILD_ROOT/bin
    [ -z "$CEPH_LIB" ] && CEPH_LIB=$CEPH_BUILD_ROOT/lib
    [ -z "$EC_PATH" ] && EC_PATH=$CEPH_LIB/erasure-code
    [ -z "$OBJCLASS_PATH" ] && OBJCLASS_PATH=$CEPH_LIB/rados-classes
    # make install should install python extensions into PYTHONPATH
elif [ -n "$CEPH_ROOT" ]; then
    [ -z "$CEPHFS_SHELL" ] && CEPHFS_SHELL=$CEPH_ROOT/src/tools/cephfs/cephfs-shell
    [ -z "$PYBIND" ] && PYBIND=$CEPH_ROOT/src/pybind
    [ -z "$CEPH_BIN" ] && CEPH_BIN=$CEPH_BUILD_DIR/bin
    [ -z "$CEPH_ADM" ] && CEPH_ADM=$CEPH_BIN/ceph
    [ -z "$INIT_CEPH" ] && INIT_CEPH=$CEPH_BIN/init-ceph
    [ -z "$CEPH_LIB" ] && CEPH_LIB=$CEPH_BUILD_DIR/lib
    [ -z "$OBJCLASS_PATH" ] && OBJCLASS_PATH=$CEPH_LIB
    [ -z "$EC_PATH" ] && EC_PATH=$CEPH_LIB
    [ -z "$CEPH_PYTHON_COMMON" ] && CEPH_PYTHON_COMMON=$CEPH_ROOT/src/python-common
fi

if [ -z "${CEPH_VSTART_WRAPPER}" ]; then
    PATH=$(pwd):$PATH
fi

[ -z "$PYBIND" ] && PYBIND=./pybind

[ -n "$CEPH_PYTHON_COMMON" ] && CEPH_PYTHON_COMMON="$CEPH_PYTHON_COMMON:"
CYTHON_PYTHONPATH="$CEPH_LIB/cython_modules/lib.${CEPH_PY_VERSION_MAJOR}"
export PYTHONPATH=$PYBIND:$CYTHON_PYTHONPATH:$CEPH_PYTHON_COMMON$PYTHONPATH

export LD_LIBRARY_PATH=$CEPH_LIB:$LD_LIBRARY_PATH
export DYLD_LIBRARY_PATH=$CEPH_LIB:$DYLD_LIBRARY_PATH
# Suppress logging for regular use that indicated that we are using a
# development version. vstart.sh is only used during testing and 
# development
export CEPH_DEV=1

[ -z "$CEPH_NUM_MON" ] && CEPH_NUM_MON="$MON"
[ -z "$CEPH_NUM_OSD" ] && CEPH_NUM_OSD="$OSD"
[ -z "$CEPH_NUM_MDS" ] && CEPH_NUM_MDS="$MDS"
[ -z "$CEPH_NUM_MGR" ] && CEPH_NUM_MGR="$MGR"
[ -z "$CEPH_NUM_FS"  ] && CEPH_NUM_FS="$FS"
[ -z "$CEPH_NUM_RGW" ] && CEPH_NUM_RGW="$RGW"

# if none of the CEPH_NUM_* number is specified, kill the existing
# cluster.
if [ -z "$CEPH_NUM_MON" -a \
     -z "$CEPH_NUM_OSD" -a \
     -z "$CEPH_NUM_MDS" -a \
     -z "$CEPH_NUM_MGR" ]; then
    kill_all=1
else
    kill_all=0
fi

[ -z "$CEPH_NUM_MON" ] && CEPH_NUM_MON=3
[ -z "$CEPH_NUM_OSD" ] && CEPH_NUM_OSD=3
[ -z "$CEPH_NUM_MDS" ] && CEPH_NUM_MDS=3
[ -z "$CEPH_NUM_MGR" ] && CEPH_NUM_MGR=1
[ -z "$CEPH_NUM_FS"  ] && CEPH_NUM_FS=1
[ -z "$CEPH_MAX_MDS" ] && CEPH_MAX_MDS=1
[ -z "$CEPH_NUM_RGW" ] && CEPH_NUM_RGW=0

[ -z "$CEPH_DIR" ] && CEPH_DIR="$PWD"
[ -z "$CEPH_DEV_DIR" ] && CEPH_DEV_DIR="$CEPH_DIR/dev"
[ -z "$CEPH_OUT_DIR" ] && CEPH_OUT_DIR="$CEPH_DIR/out"
[ -z "$CEPH_RGW_PORT" ] && CEPH_RGW_PORT=8000
[ -z "$CEPH_CONF_PATH" ] && CEPH_CONF_PATH=$CEPH_DIR

if [ $CEPH_NUM_OSD -gt 3 ]; then
    OSD_POOL_DEFAULT_SIZE=3
else
    OSD_POOL_DEFAULT_SIZE=$CEPH_NUM_OSD
fi

extra_conf=""
new=0
standby=0
debug=0
ip=""
nodaemon=0
redirect=0
smallmds=0
short=0
ec=0
ssh=0
hitset=""
overwrite_conf=1
cephx=1 #turn cephx on by default
gssapi_authx=0
cache=""
if [ `uname` = FreeBSD ]; then
    objectstore="filestore"
else
    objectstore="bluestore"
fi
ceph_osd=ceph-osd
rgw_frontend="beast"
rgw_compression=""
lockdep=${LOCKDEP:-1}
spdk_enabled=0 #disable SPDK by default
pci_id=""

with_mgr_dashboard=true
if [[ "$(get_cmake_variable WITH_MGR_DASHBOARD_FRONTEND)" != "ON" ]] ||
   [[ "$(get_cmake_variable WITH_RBD)" != "ON" ]]; then
    echo "ceph-mgr dashboard not built - disabling."
    with_mgr_dashboard=false
fi

filestore_path=
kstore_path=
bluestore_dev=

VSTART_SEC="client.vstart.sh"

MON_ADDR=""
DASH_URLS=""
RESTFUL_URLS=""

conf_fn="$CEPH_CONF_PATH/ceph.conf"
keyring_fn="$CEPH_CONF_PATH/keyring"
osdmap_fn="/tmp/ceph_osdmap.$$"
monmap_fn="/tmp/ceph_monmap.$$"
inc_osd_num=0

msgr="21"
debug_mkfs=0
debug_kvsstore=0

usage="usage: $0 [option]... \nex: MON=3 OSD=1 MDS=1 MGR=1 RGW=1 $0 -n -d\n"
usage=$usage"options:\n"
usage=$usage"\t-d, --debug\n"
usage=$usage"\t-s, --standby_mds: Generate standby-replay MDS for each active\n"
usage=$usage"\t-l, --localhost: use localhost instead of hostname\n"
usage=$usage"\t-i <ip>: bind to specific ip\n"
usage=$usage"\t-n, --new\n"
usage=$usage"\t-N, --not-new: reuse existing cluster config (default)\n"
usage=$usage"\t--valgrind[_{osd,mds,mon,rgw}] 'toolname args...'\n"
usage=$usage"\t--nodaemon: use ceph-run as wrapper for mon/osd/mds\n"
usage=$usage"\t--redirect-output: only useful with nodaemon, directs output to log file\n"
usage=$usage"\t--smallmds: limit mds cache size\n"
usage=$usage"\t-m ip:port\t\tspecify monitor address\n"
usage=$usage"\t-k keep old configuration files\n"
usage=$usage"\t-x enable cephx (on by default)\n"
usage=$usage"\t-X disable cephx\n"
usage=$usage"\t-g --gssapi enable Kerberos/GSSApi authentication\n"
usage=$usage"\t-G disable Kerberos/GSSApi authentication\n"
usage=$usage"\t--hitset <pool> <hit_set_type>: enable hitset tracking\n"
usage=$usage"\t-e : create an erasure pool\n";
usage=$usage"\t-o config\t\t add extra config parameters to all sections\n"
usage=$usage"\t--rgw_port specify ceph rgw http listen port\n"
usage=$usage"\t--rgw_frontend specify the rgw frontend configuration\n"
usage=$usage"\t--rgw_compression specify the rgw compression plugin\n"
usage=$usage"\t-b, --bluestore use bluestore as the osd objectstore backend (default)\n"
usage=$usage"\t--kvsstore use kvsstore as the osd objectstore backend\n"
usage=$usage"\t-f, --filestore use filestore as the osd objectstore backend\n"
usage=$usage"\t-K, --kstore use kstore as the osd objectstore backend\n"
usage=$usage"\t--memstore use memstore as the osd objectstore backend\n"
usage=$usage"\t--cache <pool>: enable cache tiering on pool\n"
usage=$usage"\t--short: short object names only; necessary for ext4 dev\n"
usage=$usage"\t--nolockdep disable lockdep\n"
usage=$usage"\t--multimds <count> allow multimds with maximum active count\n"
usage=$usage"\t--without-dashboard: do not run using mgr dashboard\n"
usage=$usage"\t--bluestore-spdk <vendor>:<device>: enable SPDK and specify the PCI-ID of the NVME device\n"
usage=$usage"\t--msgr1: use msgr1 only\n"
usage=$usage"\t--msgr2: use msgr2 only\n"
usage=$usage"\t--msgr21: use msgr2 and msgr1\n"
usage=$usage"\t--crimson: use crimson-osd instead of ceph-osd\n"
usage=$usage"\t--osd-args: specify any extra osd specific options\n"
usage=$usage"\t--bluestore-devs: comma-separated list of blockdevs to use for bluestore\n"
usage=$usage"\t--inc-osd: append some more osds into existing vcluster\n"
usage=$usage"\t--ssh: enable ssh orchestrator with ~/.ssh/id_rsa[.pub]\n"

usage_exit() {
    printf "$usage"
    exit
}

while [ $# -ge 1 ]; do
case $1 in
    -d | --debug )
        debug=1
        ;;
    -s | --standby_mds)
        standby=1
        ;;
    -l | --localhost )
        ip="127.0.0.1"
        ;;
    -i )
        [ -z "$2" ] && usage_exit
        ip="$2"
        shift
        ;;
    -e )
        ec=1
        ;;
    --new | -n )
        new=1
        ;;
    --inc-osd )
        new=0
        kill_all=0
        inc_osd_num=$2
        if [ "$inc_osd_num" == "" ]; then
            inc_osd_num=1
        else
            shift
        fi
        ;;
    --not-new | -N )
	new=0
	;;
    --short )
        short=1
        ;;
    --crimson )
        ceph_osd=crimson-osd
        ;;
    --osd-args )
        extra_osd_args="$2"
        shift
        ;;
    --msgr1 )
        msgr="1"
        ;;
    --msgr2 )
        msgr="2"
        ;;
    --msgr21 )
        msgr="21"
        ;;
    --ssh )
        ssh=1
        ;;
    --valgrind )
        [ -z "$2" ] && usage_exit
        valgrind=$2
        shift
        ;;
    --valgrind_args )
        valgrind_args="$2"
        shift
        ;;
    --valgrind_mds )
        [ -z "$2" ] && usage_exit
        valgrind_mds=$2
        shift
        ;;
    --valgrind_osd )
        [ -z "$2" ] && usage_exit
        valgrind_osd=$2
        shift
        ;;
    --valgrind_mon )
        [ -z "$2" ] && usage_exit
        valgrind_mon=$2
        shift
        ;;
    --valgrind_mgr )
        [ -z "$2" ] && usage_exit
        valgrind_mgr=$2
        shift
        ;;
    --valgrind_rgw )
        [ -z "$2" ] && usage_exit
        valgrind_rgw=$2
        shift
        ;;
    --nodaemon )
        nodaemon=1
        ;;
    --redirect-output)
        redirect=1
        ;;
    --smallmds )
        smallmds=1
        ;;
    --rgw_port )
        CEPH_RGW_PORT=$2
        shift
        ;;
    --rgw_frontend )
        rgw_frontend=$2
        shift
        ;;
    --rgw_compression )
        rgw_compression=$2
        shift
        ;;
    --kstore_path )
        kstore_path=$2
        shift
        ;;
    --filestore_path )
        filestore_path=$2
        shift
        ;;
    -m )
        [ -z "$2" ] && usage_exit
        MON_ADDR=$2
        shift
        ;;
    -x )
        cephx=1 # this is on be default, flag exists for historical consistency
        ;;
    -X )
        cephx=0
        ;;

    -g | --gssapi)
        gssapi_authx=1
        ;;
    -G)
        gssapi_authx=0
        ;;

    -k )
        if [ ! -r $conf_fn ]; then
            echo "cannot use old configuration: $conf_fn not readable." >&2
            exit
        fi
        overwrite_conf=0
        ;;
    --memstore )
        objectstore="memstore"
        ;;
    --kvsstore )
            objectstore="kvsstore"
        ;;
    -b | --bluestore )
        objectstore="bluestore"
        ;;
    -f | --filestore )
        objectstore="filestore"
        ;;
    -K | --kstore )
        objectstore="kstore"
        ;;
    --hitset )
        hitset="$hitset $2 $3"
        shift
        shift
        ;;
    -o )
        extra_conf="$extra_conf	$2
"
        shift
        ;;
    --cache )
        if [ -z "$cache" ]; then
            cache="$2"
        else
            cache="$cache $2"
        fi
        shift
        ;;
    --nolockdep )
        lockdep=0
        ;;
    --multimds)
        CEPH_MAX_MDS="$2"
        shift
        ;;
    --without-dashboard)
        with_mgr_dashboard=false
        ;;
    --bluestore-spdk )
        [ -z "$2" ] && usage_exit
        pci_id="$2"
        spdk_enabled=1
        shift
        ;;
    --debug-mkfs)
        debug_mkfs=1
        ;;
    --debug-kvsstore)
        debug_kvsstore=1
        ;;
    --kvsstore-dev)
        kvsstore_dev="$2"
        shift
        ;;
    --bluestore-devs )
        IFS=',' read -r -a bluestore_dev <<< "$2"
        for dev in "${bluestore_dev[@]}"; do
            if [ ! -b $dev -o ! -w $dev ]; then
                echo "All --bluestore-devs must refer to writable block devices"
                exit 1
            fi
        done
        shift
        ;;
    * )
        echo "Unknown parameter: $1" 
        usage_exit
esac
shift
done

if [ -z "$kvsstore_dev" ]; then
    echo "kvsstore_dev paramter is missing" 
    exit
fi

if [ $kill_all -eq 1 ]; then
    $SUDO $INIT_CEPH stop
fi

if [ "$overwrite_conf" -eq 0 ]; then
    CEPH_ASOK_DIR=`dirname $($CEPH_BIN/ceph-conf  -c $conf_fn --show-config-value admin_socket)`
    mkdir -p $CEPH_ASOK_DIR
    MON=`$CEPH_BIN/ceph-conf -c $conf_fn --name $VSTART_SEC --lookup num_mon 2>/dev/null` && \
        CEPH_NUM_MON="$MON"
    OSD=`$CEPH_BIN/ceph-conf -c $conf_fn --name $VSTART_SEC --lookup num_osd 2>/dev/null` && \
        CEPH_NUM_OSD="$OSD"
    MDS=`$CEPH_BIN/ceph-conf -c $conf_fn --name $VSTART_SEC --lookup num_mds 2>/dev/null` && \
        CEPH_NUM_MDS="$MDS"
    MGR=`$CEPH_BIN/ceph-conf -c $conf_fn --name $VSTART_SEC --lookup num_mgr 2>/dev/null` && \
        CEPH_NUM_MGR="$MGR"
    RGW=`$CEPH_BIN/ceph-conf -c $conf_fn --name $VSTART_SEC --lookup num_rgw 2>/dev/null` && \
        CEPH_NUM_RGW="$RGW"
else
    if [ "$new" -ne 0 ]; then
        # only delete if -n
        if [ -e "$conf_fn" ]; then
            asok_dir=`dirname $($CEPH_BIN/ceph-conf  -c $conf_fn --show-config-value admin_socket)`
            rm -- "$conf_fn"
            if [ $asok_dir != /var/run/ceph ]; then
                [ -d $asok_dir ] && rm -f $asok_dir/* && rmdir $asok_dir
            fi
        fi
        if [ -z "$CEPH_ASOK_DIR" ]; then
            CEPH_ASOK_DIR=`mktemp -u -d "${TMPDIR:-/tmp}/ceph-asok.XXXXXX"`
        fi
    else
        if [ -z "$CEPH_ASOK_DIR" ]; then
            CEPH_ASOK_DIR=`dirname $($CEPH_BIN/ceph-conf -c $conf_fn --show-config-value admin_socket)`
        fi
        # -k is implied... (doesn't make sense otherwise)
        overwrite_conf=0
    fi
fi

ARGS="-c $conf_fn"

quoted_print() {
    for s in "$@"; do
        if [[ "$s" =~ \  ]]; then
            printf -- "'%s' " "$s"
        else
            printf -- "$s "
        fi
    done
    printf '\n'
}

prunb() {
    quoted_print "$@" '&'
    "$@" &
}

prun() {
    quoted_print "$@"
    "$@"
}

prun_to_file() {
	f=$1
	shift
	quoted_print "$@"
	"$@" >> $f
}

run() {
    type=$1
    shift
    num=$1
    shift
    eval "valg=\$valgrind_$type"
    [ -z "$valg" ] && valg="$valgrind"

    if [ -n "$valg" ]; then
        prunb valgrind --tool="$valg" $valgrind_args "$@" -f
        sleep 1
    else
        if [ "$nodaemon" -eq 0 ]; then
            prun "$@"
        elif [ "$redirect" -eq 0 ]; then
            prunb ${CEPH_ROOT}/src/ceph-run "$@" -f
        else
            ( prunb ${CEPH_ROOT}/src/ceph-run "$@" -f ) >$CEPH_OUT_DIR/$type.$num.stdout 2>&1
        fi
    fi
}

wconf() {
    if [ "$overwrite_conf" -eq 1 ]; then
        cat >> "$conf_fn"
    fi
}

get_pci_selector() {
    which_pci=$1
    lspci -mm -n -D -d $pci_id | cut -d ' ' -f 1 | sed -n $which_pci'p'
}

get_pci_selector_num() {
    lspci -mm -n -D -d $pci_id | cut -d' ' -f 1 | wc -l
}

do_rgw_conf() {

    if [ $CEPH_NUM_RGW -eq 0 ]; then
        return 0
    fi

    # setup each rgw on a sequential port, starting at $CEPH_RGW_PORT.
    # individual rgw's ids will be their ports.
    current_port=$CEPH_RGW_PORT
    for n in $(seq 1 $CEPH_NUM_RGW); do
        wconf << EOF
[client.rgw.${current_port}]
        rgw frontends = $rgw_frontend port=${current_port}
        admin socket = ${CEPH_OUT_DIR}/radosgw.${current_port}.asok
EOF
        current_port=$((current_port + 1))
done

}

prepare_conf() {
    local DAEMONOPTS="
        log file = $CEPH_OUT_DIR/\$name.log
        admin socket = $CEPH_ASOK_DIR/\$name.asok
        chdir = \"\"
        pid file = $CEPH_OUT_DIR/\$name.pid
        heartbeat file = $CEPH_OUT_DIR/\$name.heartbeat
"

    local mgr_modules="restful iostat"
    if $with_mgr_dashboard; then
        mgr_modules="dashboard $mgr_modules"
    fi

    local msgr_conf=''
    if [ $msgr -eq 21 ]; then
        msgr_conf="
        ms bind msgr2 = true
        ms bind msgr1 = true
";
    fi
    if [ $msgr -eq 2 ]; then
	msgr_conf="
        ms bind msgr2 = true
        ms bind msgr1 = false
";
    fi
    if [ $msgr -eq 1 ]; then
	msgr_conf="
        ms bind msgr2 = false
        ms bind msgr1 = true
";
    fi

    wconf <<EOF
; generated by vstart.sh on `date`
[$VSTART_SEC]
        num mon = $CEPH_NUM_MON
        num osd = $CEPH_NUM_OSD
        num mds = $CEPH_NUM_MDS
        num mgr = $CEPH_NUM_MGR
        num rgw = $CEPH_NUM_RGW

[global]
        #kvsdbg_server=$kvsdbg_server
        #kvsdbg_port=$kvsdbg_port
        kvsstore_dev_path=$kvsstore_dev

        fsid = $(uuidgen)
        osd failsafe full ratio = .99
        mon osd full ratio = .99
        mon osd nearfull ratio = .99
        mon osd backfillfull ratio = .99
        erasure code dir = $EC_PATH
        plugin dir = $CEPH_LIB
        filestore fd cache size = 32
        run dir = $CEPH_OUT_DIR
        crash dir = $CEPH_OUT_DIR
        enable experimental unrecoverable data corrupting features = *
        osd_crush_chooseleaf_type = 0
        debug asok assert abort = true
$msgr_conf
$extra_conf
EOF
    if [ "$lockdep" -eq 1 ] ; then
        wconf <<EOF
        lockdep = true
EOF
    fi
    if [ "$cephx" -eq 1 ] ; then
        wconf <<EOF
        auth cluster required = cephx
        auth service required = cephx
        auth client required = cephx
EOF
    elif [ "$gssapi_authx" -eq 1 ] ; then
        wconf <<EOF
        auth cluster required = gss
        auth service required = gss
        auth client required = gss
        gss ktab client file = $CEPH_DEV_DIR/gss_\$name.keytab
EOF
    else
        wconf <<EOF
        auth cluster required = none
        auth service required = none
        auth client required = none
EOF
    fi
    if [ "$short" -eq 1 ]; then
        COSDSHORT="        osd max object name len = 460
        osd max object namespace len = 64"
    fi
    if [ "$objectstore" == "bluestore" ]; then
        if [ "$spdk_enabled" -eq 1 ]; then
            if [ "$(get_pci_selector_num)" -eq 0 ]; then
                echo "Not find the specified NVME device, please check." >&2
                exit
            fi
            if [ $(get_pci_selector_num) -lt $CEPH_NUM_OSD ]; then
                echo "OSD number ($CEPH_NUM_OSD) is greater than NVME SSD number ($(get_pci_selector_num)), please check." >&2
                exit
            fi
            BLUESTORE_OPTS="        bluestore_block_db_path = \"\"
        bluestore_block_db_size = 0
        bluestore_block_db_create = false
        bluestore_block_wal_path = \"\"
        bluestore_block_wal_size = 0
        bluestore_block_wal_create = false
        bluestore_spdk_mem = 2048"
        else
            BLUESTORE_OPTS="        bluestore block db path = $CEPH_DEV_DIR/osd\$id/block.db.file
        bluestore block db size = 1073741824
        bluestore block db create = true
        bluestore block wal path = $CEPH_DEV_DIR/osd\$id/block.wal.file
        bluestore block wal size = 1048576000
        bluestore block wal create = true"
        fi
    fi
    wconf <<EOF
[client]
        keyring = $keyring_fn
        log file = $CEPH_OUT_DIR/\$name.\$pid.log
        admin socket = $CEPH_ASOK_DIR/\$name.\$pid.asok

        ; needed for s3tests
        rgw crypt s3 kms backend = testing
        rgw crypt s3 kms encryption keys = testkey-1=YmluCmJvb3N0CmJvb3N0LWJ1aWxkCmNlcGguY29uZgo= testkey-2=aWIKTWFrZWZpbGUKbWFuCm91dApzcmMKVGVzdGluZwo=
        rgw crypt require ssl = false
        ; uncomment the following to set LC days as the value in seconds;
        ; needed for passing lc time based s3-tests (can be verbose)
        ; rgw lc debug interval = 10
        ; The following settings are for SSE-KMS with Vault
        ; rgw crypt s3 kms backend = vault
        ; rgw crypt vault auth = token
        ; rgw crypt vault addr = http://127.0.0.1:8200
        ; rgw crypt vault token file = /path/to/token.file

$extra_conf
EOF

	do_rgw_conf
	wconf << EOF
[mds]
$DAEMONOPTS
        mds data = $CEPH_DEV_DIR/mds.\$id
        mds root ino uid = `id -u`
        mds root ino gid = `id -g`
$extra_conf
[mgr]
        mgr data = $CEPH_DEV_DIR/mgr.\$id
        mgr module path = $MGR_PYTHON_PATH
        ceph daemon path = $CEPH_ROOT/src/ceph-daemon
$DAEMONOPTS
$extra_conf
[osd]
$DAEMONOPTS
        osd_check_max_object_name_len_on_startup = false
        osd data = $CEPH_DEV_DIR/osd\$id
        osd journal = $CEPH_DEV_DIR/osd\$id/journal
        osd journal size = 100
        osd class tmp = out
        osd class dir = $OBJCLASS_PATH
        osd class load list = *
        osd class default list = *

        filestore wbthrottle xfs ios start flusher = 10
        filestore wbthrottle xfs ios hard limit = 20
        filestore wbthrottle xfs inodes hard limit = 30
        filestore wbthrottle btrfs ios start flusher = 10
        filestore wbthrottle btrfs ios hard limit = 20
        filestore wbthrottle btrfs inodes hard limit = 30
        bluestore fsck on mount = true
        bluestore block create = true
$BLUESTORE_OPTS

        ; kstore
        kstore fsck on mount = true
        osd objectstore = $objectstore
$COSDSHORT
$extra_conf
[mon]
        mgr initial modules = $mgr_modules
$DAEMONOPTS
$CMONDEBUG
$extra_conf
        mon cluster log file = $CEPH_OUT_DIR/cluster.mon.\$id.log
        osd pool default erasure code profile = plugin=jerasure technique=reed_sol_van k=2 m=1 crush-failure-domain=osd
EOF
}

start_mon() {
    local MONS=""
    local count=0
    for f in a b c d e f g h i j k l m n o p q r s t u v w x y z
    do
        [ $count -eq $CEPH_NUM_MON ] && break;
        count=$(($count + 1))
        if [ -z "$MONS" ]; then
	    MONS="$f"
        else
	    MONS="$MONS $f"
        fi
    done

    if [ "$new" -eq 1 ]; then
        if [ `echo $IP | grep '^127\\.'` ]; then
            echo
            echo "NOTE: hostname resolves to loopback; remote hosts will not be able to"
            echo "  connect.  either adjust /etc/hosts, or edit this script to use your"
            echo "  machine's real IP."
            echo
        fi

        prun $SUDO "$CEPH_BIN/ceph-authtool" --create-keyring --gen-key --name=mon. "$keyring_fn" --cap mon 'allow *'
        prun $SUDO "$CEPH_BIN/ceph-authtool" --gen-key --name=client.admin \
             --cap mon 'allow *' \
             --cap osd 'allow *' \
             --cap mds 'allow *' \
             --cap mgr 'allow *' \
             "$keyring_fn"

        prun $SUDO "$CEPH_BIN/ceph-authtool" --gen-key --name=client.fs\
             --cap mon 'allow r' \
             --cap osd 'allow rw tag cephfs data=*' \
             --cap mds 'allow rwp' \
             "$keyring_fn"

        # build a fresh fs monmap, mon fs
        local params=()
        local count=0
        local mon_host=""
        for f in $MONS
        do
            if [ $msgr -eq 1 ]; then
                A="v1:$IP:$(($CEPH_PORT+$count+1))"
            fi
            if [ $msgr -eq 2 ]; then
                A="v2:$IP:$(($CEPH_PORT+$count+1))"
            fi
            if [ $msgr -eq 21 ]; then
                A="[v2:$IP:$(($CEPH_PORT+$count)),v1:$IP:$(($CEPH_PORT+$count+1))]"
            fi
            params+=("--addv" "$f" "$A")
            mon_host="$mon_host $A"
            wconf <<EOF
[mon.$f]
        host = $HOSTNAME
        mon data = $CEPH_DEV_DIR/mon.$f
EOF
            count=$(($count + 2))
        done
        wconf <<EOF
[global]
        mon host = $mon_host
EOF
        prun "$CEPH_BIN/monmaptool" --create --clobber "${params[@]}" --print "$monmap_fn"

        for f in $MONS
        do
            prun rm -rf -- "$CEPH_DEV_DIR/mon.$f"
            prun mkdir -p "$CEPH_DEV_DIR/mon.$f"
            prun "$CEPH_BIN/ceph-mon" --mkfs -c "$conf_fn" -i "$f" --monmap="$monmap_fn" --keyring="$keyring_fn"
        done

        prun rm -- "$monmap_fn"
    fi

    # start monitors
    for f in $MONS
    do
        run 'mon' $f $CEPH_BIN/ceph-mon -i $f $ARGS $CMON_ARGS
    done
}

start_osd() {
    if [ $inc_osd_num -gt 0 ]; then
        old_maxosd=$($CEPH_BIN/ceph osd getmaxosd | sed -e 's/max_osd = //' -e 's/ in epoch.*//')
        start=$old_maxosd
        end=$(($start-1+$inc_osd_num))
        overwrite_conf=1 # fake wconf
    else
        start=0
        end=$(($CEPH_NUM_OSD-1))
    fi
    for osd in `seq $start $end`
    do
	if [ "$new" -eq 1 -o $inc_osd_num -gt 0 ]; then
            wconf <<EOF
[osd.$osd]
        host = $HOSTNAME
EOF
            if [ "$spdk_enabled" -eq 1 ]; then
                wconf <<EOF
        bluestore_block_path = spdk:$(get_pci_selector $((osd+1)))
EOF
            fi

            rm -rf $CEPH_DEV_DIR/osd$osd || true
            if command -v btrfs > /dev/null; then
                for f in $CEPH_DEV_DIR/osd$osd/*; do btrfs sub delete $f &> /dev/null || true; done
            fi
            if [ -n "$filestore_path" ]; then
                ln -s $filestore_path $CEPH_DEV_DIR/osd$osd
            elif [ -n "$kstore_path" ]; then
                ln -s $kstore_path $CEPH_DEV_DIR/osd$osd
            else
                mkdir -p $CEPH_DEV_DIR/osd$osd
                if [ -n "${bluestore_dev[$osd]}" ]; then
                    dd if=/dev/zero of=${bluestore_dev[$osd]} bs=1M count=1
                    ln -s ${bluestore_dev[$osd]} $CEPH_DEV_DIR/osd$osd/block
                    wconf <<EOF
        bluestore fsck on mount = false
EOF
                fi
            fi

            local uuid=`uuidgen`
            echo "add osd$osd $uuid"
            OSD_SECRET=$($CEPH_BIN/ceph-authtool --gen-print-key)
            echo "{\"cephx_secret\": \"$OSD_SECRET\"}" > $CEPH_DEV_DIR/osd$osd/new.json
            ceph_adm osd new $uuid -i $CEPH_DEV_DIR/osd$osd/new.json
            rm $CEPH_DEV_DIR/osd$osd/new.json

            if [ $debug_mkfs -eq 1 ]; then
            #    echo "$SUDO gdbserver localhost:7788 $CEPH_BIN/$ceph_osd $extra_osd_args -i $osd $ARGS --mkfs --key $OSD_SECRET --osd-uuid $uuid"
            #    sudo env PATH="$PATH" gdbserver localhost:7788 $CEPH_BIN/$ceph_osd $extra_osd_args -i $osd $ARGS --mkfs --key $OSD_SECRET --osd-uuid $uuid
                echo "$SUDO $CEPH_BIN/$ceph_osd $extra_osd_args -i $osd $ARGS --mkfs --key $OSD_SECRET --osd-uuid $uuid"
                exit
            else
                echo "$SUDO $CEPH_BIN/$ceph_osd $extra_osd_args -i $osd $ARGS --mkfs --key $OSD_SECRET --osd-uuid $uuid"
                $SUDO $CEPH_BIN/$ceph_osd $extra_osd_args -i $osd $ARGS --mkfs --key $OSD_SECRET --osd-uuid $uuid
            fi

            local key_fn=$CEPH_DEV_DIR/osd$osd/keyring
            cat > $key_fn<<EOF
[osd.$osd]
        key = $OSD_SECRET
EOF
        fi
        echo start osd.$osd
        local extra_seastar_args
        if [ "$ceph_osd" == "crimson-osd" ]; then
            # designate a single CPU node $osd for osd.$osd
            extra_seastar_args="--smp 1 --cpuset $osd"
        fi
       
        if [ $debug_kvsstore -eq 1 ]; then 
            #echo "sudo gdbserver localhost:7788 $CEPH_BIN/$ceph_osd $extra_seastar_args $extra_osd_args -i $osd $ARGS $COSD_ARGS"
            echo "$CEPH_BIN/$ceph_osd $extra_seastar_args $extra_osd_args -i $osd $ARGS $COSD_ARGS"
            exit          
        else
            run 'osd' $osd $SUDO $CEPH_BIN/$ceph_osd \
                $extra_seastar_args $extra_osd_args \
                -i $osd $ARGS $COSD_ARGS
        fi
    done
    if [ $inc_osd_num -gt 0 ]; then
        # update num osd
        new_maxosd=$($CEPH_BIN/ceph osd getmaxosd | sed -e 's/max_osd = //' -e 's/ in epoch.*//')
        sed -i "s/num osd = .*/num osd = $new_maxosd/g" $conf_fn
    fi
}

start_mgr() {
    local mgr=0
    local ssl=${DASHBOARD_SSL:-1}
    # avoid monitors on nearby ports (which test/*.sh use extensively)
    MGR_PORT=$(($CEPH_PORT + 1000))
    for name in x y z a b c d e f g h i j k l m n o p
    do
        [ $mgr -eq $CEPH_NUM_MGR ] && break
        mgr=$(($mgr + 1))
        if [ "$new" -eq 1 ]; then
            mkdir -p $CEPH_DEV_DIR/mgr.$name
            key_fn=$CEPH_DEV_DIR/mgr.$name/keyring
            $SUDO $CEPH_BIN/ceph-authtool --create-keyring --gen-key --name=mgr.$name $key_fn
            ceph_adm -i $key_fn auth add mgr.$name mon 'allow profile mgr' mds 'allow *' osd 'allow *'

            wconf <<EOF
[mgr.$name]
        host = $HOSTNAME
EOF

            if $with_mgr_dashboard ; then
                local port_option="ssl_server_port"
                local http_proto="https"
                if [ "$ssl" == "0" ]; then
                    port_option="server_port"
                    http_proto="http"
                    ceph_adm config set mgr mgr/dashboard/ssl false --force
                fi
                ceph_adm config set mgr mgr/dashboard/$name/$port_option $MGR_PORT --force
                if [ $mgr -eq 1 ]; then
                    DASH_URLS="$http_proto://$IP:$MGR_PORT"
                else
                    DASH_URLS+=", $http_proto://$IP:$MGR_PORT"
                fi
            fi
	    MGR_PORT=$(($MGR_PORT + 1000))

	    ceph_adm config set mgr mgr/restful/$name/server_port $MGR_PORT --force
            if [ $mgr -eq 1 ]; then
                RESTFUL_URLS="https://$IP:$MGR_PORT"
            else
                RESTFUL_URLS+=", https://$IP:$MGR_PORT"
            fi
	    MGR_PORT=$(($MGR_PORT + 1000))
        fi

        echo "Starting mgr.${name}"
        run 'mgr' $name $CEPH_BIN/ceph-mgr -i $name $ARGS
    done

    if [ "$new" -eq 1 ]; then
        # setting login credentials for dashboard
        if $with_mgr_dashboard; then
            while ! ceph_adm -h | grep -c -q ^dashboard ; do
                echo 'waiting for mgr dashboard module to start'
                sleep 1
            done
            ceph_adm dashboard ac-user-create admin admin administrator
            if [ "$ssl" != "0" ]; then
                if ! ceph_adm dashboard create-self-signed-cert;  then
                    echo dashboard module not working correctly!
                fi
            fi
        fi

        while ! ceph_adm -h | grep -c -q ^restful ; do
            echo 'waiting for mgr restful module to start'
            sleep 1
        done
        if ceph_adm restful create-self-signed-cert; then
            SF=`mktemp`
            ceph_adm restful create-key admin -o $SF
            RESTFUL_SECRET=`cat $SF`
            rm $SF
        else
            echo MGR Restful is not working, perhaps the package is not installed?
        fi
    fi

    if [ "$ssh" -eq 1 ]; then
        echo Enabling ssh orchestrator
        ceph_adm config-key set mgr/ssh/ssh_identity_key -i ~/.ssh/id_rsa
        ceph_adm config-key set mgr/ssh/ssh_identity_pub -i ~/.ssh/id_rsa.pub
        ceph_adm mgr module enable ssh
        ceph_adm orchestrator set backend ssh
        ceph_adm orchestrator host add $HOSTNAME
    fi
}

start_mds() {
    local mds=0
    for name in a b c d e f g h i j k l m n o p
    do
        [ $mds -eq $CEPH_NUM_MDS ] && break
        mds=$(($mds + 1))

        if [ "$new" -eq 1 ]; then
            prun mkdir -p "$CEPH_DEV_DIR/mds.$name"
            key_fn=$CEPH_DEV_DIR/mds.$name/keyring
            wconf <<EOF
[mds.$name]
        host = $HOSTNAME
EOF
            if [ "$standby" -eq 1 ]; then
                mkdir -p $CEPH_DEV_DIR/mds.${name}s
                wconf <<EOF
        mds standby for rank = $mds
[mds.${name}s]
        mds standby replay = true
        mds standby for name = ${name}
EOF
            fi
            prun $SUDO "$CEPH_BIN/ceph-authtool" --create-keyring --gen-key --name="mds.$name" "$key_fn"
            ceph_adm -i "$key_fn" auth add "mds.$name" mon 'allow profile mds' osd 'allow rw tag cephfs *=*' mds 'allow' mgr 'allow profile mds'
            if [ "$standby" -eq 1 ]; then
                prun $SUDO "$CEPH_BIN/ceph-authtool" --create-keyring --gen-key --name="mds.${name}s" \
                     "$CEPH_DEV_DIR/mds.${name}s/keyring"
                ceph_adm -i "$CEPH_DEV_DIR/mds.${name}s/keyring" auth add "mds.${name}s" \
                             mon 'allow profile mds' osd 'allow *' mds 'allow' mgr 'allow profile mds'
            fi
        fi

        run 'mds' $name $CEPH_BIN/ceph-mds -i $name $ARGS $CMDS_ARGS
        if [ "$standby" -eq 1 ]; then
            run 'mds' $name $CEPH_BIN/ceph-mds -i ${name}s $ARGS $CMDS_ARGS
        fi

        #valgrind --tool=massif $CEPH_BIN/ceph-mds $ARGS --mds_log_max_segments 2 --mds_thrash_fragments 0 --mds_thrash_exports 0 > m  #--debug_ms 20
        #$CEPH_BIN/ceph-mds -d $ARGS --mds_thrash_fragments 0 --mds_thrash_exports 0 #--debug_ms 20
        #ceph_adm mds set max_mds 2
    done

    if [ $new -eq 1 ]; then
        if [ "$CEPH_NUM_FS" -gt "0" ] ; then
            sleep 5 # time for MDS to come up as standby to avoid health warnings on fs creation
            if [ "$CEPH_NUM_FS" -gt "1" ] ; then
                ceph_adm fs flag set enable_multiple true --yes-i-really-mean-it
            fi

	    # wait for volume module to load
	    while ! ceph_adm fs volume ls ; do sleep 1 ; done

            local fs=0
            for name in a b c d e f g h i j k l m n o p
            do
                ceph_adm fs volume create ${name}
                ceph_adm fs authorize ${name} "client.fs_${name}" / rwp
                fs=$(($fs + 1))
                [ $fs -eq $CEPH_NUM_FS ] && break
            done
        fi
    fi

}

if [ "$debug" -eq 0 ]; then
    CMONDEBUG='
        debug mon = 10
        debug ms = 1'
    COSDDEBUG='
        debug ms = 0
        debug kvsstore = 30'
else
    echo "** going verbose **"
    CMONDEBUG='
        debug mon = 20
        debug paxos = 20
        debug auth = 20
        debug mgrc = 20
        debug ms = 1'
fi

if [ -n "$MON_ADDR" ]; then
    CMON_ARGS=" -m "$MON_ADDR
    COSD_ARGS=" -m "$MON_ADDR
    CMDS_ARGS=" -m "$MON_ADDR
fi

if [ -z "$CEPH_PORT" ]; then
    while [ true ]
    do
        CEPH_PORT="$(echo $(( RANDOM % 1000 + 40000 )))"
        ss -a -n | egrep "\<LISTEN\>.+:${CEPH_PORT}\s+" 1>/dev/null 2>&1 || break
    done
fi

[ -z "$INIT_CEPH" ] && INIT_CEPH=$CEPH_BIN/init-ceph

# sudo if btrfs
[ -d $CEPH_DEV_DIR/osd0/. ] && [ -e $CEPH_DEV_DIR/sudo ] && SUDO="sudo"

if [ $inc_osd_num -eq 0 ]; then
    prun $SUDO rm -f core*
fi

[ -d $CEPH_ASOK_DIR ] || mkdir -p $CEPH_ASOK_DIR
[ -d $CEPH_OUT_DIR  ] || mkdir -p $CEPH_OUT_DIR
[ -d $CEPH_DEV_DIR  ] || mkdir -p $CEPH_DEV_DIR
if [ $inc_osd_num -eq 0 ]; then
    $SUDO rm -rf $CEPH_OUT_DIR/*
fi
[ -d gmon ] && $SUDO rm -rf gmon/*

[ "$cephx" -eq 1 ] && [ "$new" -eq 1 ] && [ -e $keyring_fn ] && rm $keyring_fn


# figure machine's ip
HOSTNAME=`hostname -s`
if [ -n "$ip" ]; then
    IP="$ip"
else
    echo hostname $HOSTNAME
    if [ -x "$(which ip 2>/dev/null)" ]; then
        IP_CMD="ip addr"
    else
        IP_CMD="ifconfig"
    fi
    # filter out IPv4 and localhost addresses
    IP="$($IP_CMD | sed -En 's/127.0.0.1//;s/.*inet (addr:)?(([0-9]*\.){3}[0-9]*).*/\2/p' | head -n1)"
    # if nothing left, try using localhost address, it might work
    if [ -z "$IP" ]; then IP="127.0.0.1"; fi
fi
echo "ip $IP"
echo "port $CEPH_PORT"


[ -z $CEPH_ADM ] && CEPH_ADM=$CEPH_BIN/ceph

ceph_adm() {
    if [ "$cephx" -eq 1 ]; then
        prun $SUDO "$CEPH_ADM" -c "$conf_fn" -k "$keyring_fn" "$@"
    else
        prun $SUDO "$CEPH_ADM" -c "$conf_fn" "$@"
    fi
}

ceph_adm_to_file() {
	f=$1
	shift

	if [ "$cephx" -eq 1 ]; then
		prun_to_file $f $SUDO "$CEPH_ADM" -c "$conf_fn" -k "$keyring_fn" "$@"
	else
		prun_to_file $f $SUDO "$CEPH_ADM" -c "$conf_fn" "$@"
	fi
}

if [ $inc_osd_num -gt 0 ]; then
    start_osd
    exit
fi

if [ "$new" -eq 1 ]; then
    prepare_conf
fi

if [ $CEPH_NUM_MON -gt 0 ]; then
    start_mon

    echo Populating config ...
    cat <<EOF | $CEPH_BIN/ceph -c $conf_fn config assimilate-conf -i -
[global]
osd_pool_default_size = $OSD_POOL_DEFAULT_SIZE
osd_pool_default_min_size = 1

[mon]
mon_osd_reporter_subtree_level = osd
mon_data_avail_warn = 2
mon_data_avail_crit = 1
mon_allow_pool_delete = true

[osd]
osd_scrub_load_threshold = 2000
osd_debug_op_order = true
osd_debug_misdirected_ops = true
osd_copyfrom_max_chunk = 524288

[mds]
mds_debug_frag = true
mds_debug_auth_pins = true
mds_debug_subtrees = true

[mgr]
mgr/telemetry/nag = false
mgr/telemetry/enable = false

EOF

    if [ "$debug" -ne 0 ]; then
        echo Setting debug configs ...
        cat <<EOF | $CEPH_BIN/ceph -c $conf_fn config assimilate-conf -i -
[mgr]
debug_ms = 1
debug_mgr = 20
debug_monc = 20
debug_mon = 20

[osd]
debug_ms = 1
debug_osd = 25
debug_objecter = 20
debug_monc = 20
debug_mgrc = 20
debug_journal = 20
debug_filestore = 20
debug_bluestore = 20
debug_bluefs = 20
debug_rocksdb = 20
debug_bdev = 20
debug_reserver = 10
debug_objclass = 20

[mds]
debug_ms = 1
debug_mds = 20
debug_monc = 20
debug_mgrc = 20
mds_debug_scatterstat = true
mds_verify_scatter = true
EOF
    fi
fi

if [ $CEPH_NUM_MGR -gt 0 ]; then
    start_mgr
fi

# osd
if [ $CEPH_NUM_OSD -gt 0 ]; then
    start_osd
fi

# mds
if [ "$smallmds" -eq 1 ]; then
    wconf <<EOF
[mds]
        mds log max segments = 2
        mds cache size = 10000
EOF
fi

if [ $CEPH_NUM_MDS -gt 0 ]; then
    start_mds
fi

# Don't set max_mds until all the daemons are started, otherwise
# the intended standbys might end up in active roles.
if [ "$CEPH_MAX_MDS" -gt 1 ]; then
    sleep 5  # wait for daemons to make it into FSMap before increasing max_mds
fi
fs=0
for name in a b c d e f g h i j k l m n o p
do
    [ $fs -eq $CEPH_NUM_FS ] && break
    fs=$(($fs + 1))
    if [ "$CEPH_MAX_MDS" -gt 1 ]; then
        ceph_adm fs set "cephfs_${name}" max_mds "$CEPH_MAX_MDS"
    fi
done

# mgr

if [ "$ec" -eq 1 ]; then
    ceph_adm <<EOF
osd erasure-code-profile set ec-profile m=2 k=2
osd pool create ec erasure ec-profile
EOF
fi

do_cache() {
    while [ -n "$*" ]; do
        p="$1"
        shift
        echo "creating cache for pool $p ..."
        ceph_adm <<EOF
osd pool create ${p}-cache
osd tier add $p ${p}-cache
osd tier cache-mode ${p}-cache writeback
osd tier set-overlay $p ${p}-cache
EOF
    done
}
do_cache $cache

do_hitsets() {
    while [ -n "$*" ]; do
        pool="$1"
        type="$2"
        shift
        shift
        echo "setting hit_set on pool $pool type $type ..."
        ceph_adm <<EOF
osd pool set $pool hit_set_type $type
osd pool set $pool hit_set_count 8
osd pool set $pool hit_set_period 30
EOF
    done
}
do_hitsets $hitset

do_rgw_create_users()
{
    # Create S3 user
    local akey='0555b35654ad1656d804'
    local skey='h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='
    echo "setting up user testid"
    $CEPH_BIN/radosgw-admin user create --uid testid --access-key $akey --secret $skey --display-name 'M. Tester' --email tester@ceph.com -c $conf_fn > /dev/null

    # Create S3-test users
    # See: https://github.com/ceph/s3-tests
    echo "setting up s3-test users"
    $CEPH_BIN/radosgw-admin user create \
        --uid 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef \
        --access-key ABCDEFGHIJKLMNOPQRST \
        --secret abcdefghijklmnopqrstuvwxyzabcdefghijklmn \
        --display-name youruseridhere \
        --email s3@example.com -c $conf_fn > /dev/null
    $CEPH_BIN/radosgw-admin user create \
        --uid 56789abcdef0123456789abcdef0123456789abcdef0123456789abcdef01234 \
        --access-key NOPQRSTUVWXYZABCDEFG \
        --secret nopqrstuvwxyzabcdefghijklmnabcdefghijklm \
        --display-name john.doe \
        --email john.doe@example.com -c $conf_fn > /dev/null
    $CEPH_BIN/radosgw-admin user create \
	--tenant testx \
        --uid 9876543210abcdef0123456789abcdef0123456789abcdef0123456789abcdef \
        --access-key HIJKLMNOPQRSTUVWXYZA \
        --secret opqrstuvwxyzabcdefghijklmnopqrstuvwxyzab \
        --display-name tenanteduser \
        --email tenanteduser@example.com -c $conf_fn > /dev/null

    # Create Swift user
    echo "setting up user tester"
    $CEPH_BIN/radosgw-admin user create -c $conf_fn --subuser=test:tester --display-name=Tester-Subuser --key-type=swift --secret=testing --access=full > /dev/null

    echo ""
    echo "S3 User Info:"
    echo "  access key:  $akey"
    echo "  secret key:  $skey"
    echo ""
    echo "Swift User Info:"
    echo "  account   : test"
    echo "  user      : tester"
    echo "  password  : testing"
    echo ""
}

do_rgw()
{
    if [ "$new" -eq 1 ]; then
        do_rgw_create_users
        if [ -n "$rgw_compression" ]; then
            echo "setting compression type=$rgw_compression"
            $CEPH_BIN/radosgw-admin zone placement modify -c $conf_fn --rgw-zone=default --placement-id=default-placement --compression=$rgw_compression > /dev/null
        fi
    fi
    # Start server
    RGWDEBUG=""
    if [ "$debug" -ne 0 ]; then
        RGWDEBUG="--debug-rgw=20 --debug-ms=1"
    fi

    local CEPH_RGW_PORT_NUM="${CEPH_RGW_PORT}"
    local CEPH_RGW_HTTPS="${CEPH_RGW_PORT: -1}"
    if [[ "${CEPH_RGW_HTTPS}" = "s" ]]; then
        CEPH_RGW_PORT_NUM="${CEPH_RGW_PORT::-1}"
    else
        CEPH_RGW_HTTPS=""
    fi
    RGWSUDO=
    [ $CEPH_RGW_PORT_NUM -lt 1024 ] && RGWSUDO=sudo

    current_port=$CEPH_RGW_PORT
    for n in $(seq 1 $CEPH_NUM_RGW); do
        rgw_name="client.rgw.${current_port}"

        ceph_adm_to_file $keyring_fn auth get-or-create $rgw_name \
            mon 'allow rw' \
            osd 'allow rwx' \
            mgr 'allow rw' \

        echo start rgw on http${CEPH_RGW_HTTPS}://localhost:${current_port}
        run 'rgw' $current_port $RGWSUDO $CEPH_BIN/radosgw -c $conf_fn \
            --log-file=${CEPH_OUT_DIR}/radosgw.${current_port}.log \
            --admin-socket=${CEPH_OUT_DIR}/radosgw.${current_port}.asok \
            --pid-file=${CEPH_OUT_DIR}/radosgw.${current_port}.pid \
            ${RGWDEBUG} \
            -n ${rgw_name} \
            "--rgw_frontends=${rgw_frontend} port=${current_port}${CEPH_RGW_HTTPS}"

        i=$(($i + 1))
        [ $i -eq $CEPH_NUM_RGW ] && break

        current_port=$((current_port+1))
    done
}
if [ "$CEPH_NUM_RGW" -gt 0 ]; then
    do_rgw
fi

echo "started.  stop.sh to stop.  see out/* (e.g. 'tail -f out/????') for debug output."

echo ""
if [ "$new" -eq 1 ]; then
    if $with_mgr_dashboard; then
        echo "dashboard urls: $DASH_URLS"
        echo "  w/ user/pass: admin / admin"
    fi
    echo "restful urls: $RESTFUL_URLS"
    echo "  w/ user/pass: admin / $RESTFUL_SECRET"
    echo ""
fi
echo ""
# add header to the environment file
{
    echo "#"
    echo "# source this file into your shell to set up the environment."
    echo "# For example:"
    echo "# $ . $CEPH_DIR/vstart_environment.sh"
    echo "#"
} > $CEPH_DIR/vstart_environment.sh
{
    echo "export PYTHONPATH=$PYBIND:$CYTHON_PYTHONPATH:$CEPH_PYTHON_COMMON\$PYTHONPATH"
    echo "export LD_LIBRARY_PATH=$CEPH_LIB:\$LD_LIBRARY_PATH"

    if [ "$CEPH_DIR" != "$PWD" ]; then
        echo "export CEPH_CONF=$conf_fn"
        echo "export CEPH_KEYRING=$keyring_fn"
    fi

    if [ -n "$CEPHFS_SHELL" ]; then
        echo "alias cephfs-shell=$CEPHFS_SHELL"
    fi
} | tee -a $CEPH_DIR/vstart_environment.sh

echo "CEPH_DEV=1"
