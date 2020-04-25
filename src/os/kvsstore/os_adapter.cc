#include "common/errno.h"
#include "common/dout.h"
#include "common/debug.h"
#include "os_adapter.h"

#define dout_context cct
#define dout_subsys ceph_subsys_kvs

#undef dout_prefix
#define dout_prefix *_dout << "[kvs] "

int ObjectStoreAdapter::mkfs() {
    FTRACE

    int r;
    uuid_d old_fsid;
    r = _open_path();
    if (r < 0)
        return r;
    r = _open_fsid(true);
    if (r < 0)
        goto out_path_fd;

    r = _lock_fsid();
    if (r < 0)
        goto out_close_fsid;

    r = _read_fsid(&old_fsid);
    if (r < 0 || old_fsid.is_zero()) {
        if (fsid.is_zero()) {
            fsid.generate_random();
        } else {
        }
        // we'll write it last.
    } else {
        if (!fsid.is_zero() && fsid != old_fsid) {
            r = -EINVAL;
            goto out_close_fsid;
        }
        fsid = old_fsid;
        goto out_close_fsid;
    }

    r = open_db(true);
    if (r < 0)
        goto out_close_fsid;

    mkfs_kvsstore();

    r = write_sb();
    // derr << __func__ << " write_sb r = " << r << dendl;
    if (r < 0)
        goto outclose_db;

    r = write_meta("type", "kvsstore");
    if (r < 0)
        goto outclose_db;

    // indicate mkfs completion/success by writing the fsid file
    r = _write_fsid();
    if (r == 0)
        dout(10) << __func__ << " success" << dendl;
    else
        derr << __func__ << " error writing fsid: " << cpp_strerror(r) << dendl;

    outclose_db: close_db();

    out_close_fsid: _close_fsid();

    out_path_fd: _close_path();

    return r;
}


bool ObjectStoreAdapter::test_mount_in_use() {
    FTRACE
    // most error conditions mean the mount is not in use (e.g., because
    // it doesn't exist).  only if we fail to lock do we conclude it is
    // in use.
    bool ret = false;
    int r = _open_path();
    if (r < 0)
        return false;
    r = _open_fsid(false);
    if (r < 0)
        goto out_path;
    r = _lock_fsid();
    if (r < 0)
        ret = true; // if we can't lock, it is in use
    _close_fsid();
    out_path: _close_path();
    return ret;
}


int ObjectStoreAdapter::_open_path() {
    FTRACE
    assert(path_fd < 0);
    path_fd = ::open(path.c_str(), O_DIRECTORY);
    if (path_fd < 0) {
        return -errno;
    }
    return 0;
}

void ObjectStoreAdapter::_close_path() {
    FTRACE
    VOID_TEMP_FAILURE_RETRY(::close(path_fd));
    path_fd = -1;
}

int ObjectStoreAdapter::_open_fsid(bool create) {
    FTRACE
    assert(fsid_fd < 0);
    int flags = O_RDWR;
    if (create)
        flags |= O_CREAT;
    fsid_fd = ::openat(path_fd, "fsid", flags, 0644);
    if (fsid_fd < 0) {
        int err = -errno;
        derr << __func__ << " " << cpp_strerror(err) << dendl;
        return err;
    }
    return 0;
}

int ObjectStoreAdapter::_read_fsid(uuid_d *uuid) {
    FTRACE
    char fsid_str[40];
    memset(fsid_str, 0, sizeof(fsid_str));
    int ret = safe_read(fsid_fd, fsid_str, sizeof(fsid_str));
    if (ret < 0) {
        derr << __func__ << " failed: " << cpp_strerror(ret) << dendl;
        return ret;
    }
    if (ret > 36)
        fsid_str[36] = 0;
    else
        fsid_str[ret] = 0;
    if (!uuid->parse(fsid_str)) {
        derr << __func__ << " unparsable uuid " << fsid_str << dendl;
        return -EINVAL;
    }
    return 0;
}

int ObjectStoreAdapter::_write_fsid() {
    FTRACE
    int r = ::ftruncate(fsid_fd, 0);
    if (r < 0) {
        r = -errno;
        derr << __func__ << " fsid truncate failed: " << cpp_strerror(r) << dendl;
        return r;
    }
    string str = stringify(fsid) + "\n";
    r = safe_write(fsid_fd, str.c_str(), str.length());
    if (r < 0) {
        derr << __func__ << " fsid write failed: " << cpp_strerror(r) << dendl;
        return r;
    }
    r = ::fsync(fsid_fd);
    if (r < 0) {
        r = -errno;
        derr << __func__ << " fsid fsync failed: " << cpp_strerror(r) << dendl;
        return r;
    }
    return 0;
}

void ObjectStoreAdapter::_close_fsid() {
    FTRACE
    VOID_TEMP_FAILURE_RETRY(::close(fsid_fd));
    fsid_fd = -1;
}

int ObjectStoreAdapter::_lock_fsid() {
    FTRACE
    struct flock l;
    memset(&l, 0, sizeof(l));
    l.l_type = F_WRLCK;
    l.l_whence = SEEK_SET;
    int r = ::fcntl(fsid_fd, F_SETLK, &l);
    if (r < 0) {
        int err = errno;
        derr << __func__ << " failed to lock " << path << "/fsid"
             << " (is another ceph-osd still running?)"
             << cpp_strerror(err) << dendl;
        return -err;
    }
    return 0;
}


int ObjectStoreAdapter::mount() {
    FTRACE

    TR   <<  "Mount ----------------------------------------  " ;
    int r = _open_path();
    if (r < 0)
        return r;
    r = _open_fsid(false);
    if (r < 0)
        goto out_path;

    r = _read_fsid(&fsid);
    if (r < 0)
        goto out_fsid;

    r = _lock_fsid();
    if (r < 0)
        goto out_fsid;

    r = open_db(false);
    if (r < 0)
        goto out_fsid;

    r = fsck_impl();

    if (r < 0)
        goto out_db;

    mount_kvsstore();

    r = write_sb();
    derr << __func__ << " new KVSB superblock is written, ret = " << r << dendl;
    if (r < 0)
        goto out_db;

    r = open_collections();
    derr << __func__ << " _open_collections, ret = " << r << dendl;
    if (r < 0)
        goto out_db;

    mounted = true;


    derr <<  "Mounted " << dendl;
    TR   <<  "Mounted ----------------------------------------  " ;

    return 0;

    TR   <<  "Mount failed ----------------------------------------  " ;

    out_db: close_db();
    out_fsid: _close_fsid();
    out_path: _close_path();

    return r;
}

int ObjectStoreAdapter::_fsck_with_mount() {
    FTRACE
    int r = this->mount();  // includes fsck
    if (r < 0)
        return r;
    r = this->umount();
    if (r < 0)
        return r;
    return 0;
}


int ObjectStoreAdapter::umount() {
    FTRACE

    assert(mounted);
    osr_drain_all();
    mounted = false;

    umount_kvsstore();

    reap_collections();
    flush_cache();

    _close_fsid();
    _close_path();

    return 0;
}

int ObjectStoreAdapter::collection_empty(CollectionHandle &ch, bool *empty)  {
    FTRACE
    dout(15) << __func__ << " " << ch->cid << dendl;
    vector<ghobject_t> ls;
    ghobject_t next;
    int r = collection_list(ch, ghobject_t(), ghobject_t::get_max(), 1, &ls,
                            &next);
    if (r < 0) {
    derr << __func__ << " collection_list returned: "
    << cpp_strerror(r) << dendl;
    return r;
    }
    *empty = ls.empty();
    dout(10) << __func__ << " " << ch->cid << " = " << (int) (*empty) << dendl;
    return 0;
};


int ObjectStoreAdapter::collection_bits(CollectionHandle &ch)  {
    FTRACE
    dout(15) << __func__ << " " << ch->cid << dendl;
    Collection *c = static_cast<Collection*>(ch.get());
    
    std::shared_lock l(c->lock);
    
    dout(10) << __func__ << " " << ch->cid << " = " << c->cnode.bits << dendl;
    
    return c->cnode.bits;
}
int ObjectStoreAdapter::fiemap(CollectionHandle &c_, const ghobject_t &oid,
                               uint64_t offset, size_t length, bufferlist &bl) {
    FTRACE
    map<uint64_t, uint64_t> m;
    int r = fiemap(c_, oid, offset, length, m);
    if (r >= 0) {
        encode(m, bl);
    }
    return r;
}

int ObjectStoreAdapter::fiemap(CollectionHandle &c_, const ghobject_t &oid,
                               uint64_t offset, size_t length, map<uint64_t, uint64_t> &destmap) {
    FTRACE
    int r = fiemap_impl(c_, oid, offset, length, destmap);
    if (r < 0) {
    destmap.clear();
    }
    return r;
}