/*
 * kvsstore_iter.h
 *
 *  Created on: Nov 17, 2019
 *      Author: root
 */

#ifndef SRC_OS_KVSSTORE_KVSSTORE_INORDER_ITERATOR_H_
#define SRC_OS_KVSSTORE_KVSSTORE_INORDER_ITERATOR_H_


#define dout_context cct
#define dout_subsys ceph_subsys_kvs

template<class T, class DB>
class LsCache
{
    class LsData
    {
        std::mutex d_lock;// = ceph::make_mutex("KvsStore::d_lock");
        std::mutex i_lock;// = ceph::make_mutex("KvsStore::i_lock");
        bool loaded;
        bool buffering;
    public:
        std::set<T> data;
        std::vector<bool> incoming_op_w;
        std::vector<T> incoming_data;

        uint64_t poolid;
        int8_t shardid;
        utime_t ttl;
        CephContext *cct;

        LsData(CephContext *cct_, uint64_t &pid, int8_t sid):  loaded(false),buffering(false), poolid(pid), shardid(sid),cct(cct_) {}

        utime_t fill(DB *db, uint8_t space_id) {
            std::unique_lock lk{d_lock};
            if (loaded) {
                return utime_t();
            }

            utime_t st = ceph_clock_now();

            buffering = true;
            db->iterate_objects_in_device(poolid, shardid, data, space_id);
            buffering = false;

            utime_t et = ceph_clock_now();

            ttl = et;
            ttl += 60*2;

            apply_outstanding_requests();

            loaded = true;

            return et-st;
        }

        int apply_outstanding_requests()
        {
            std::unique_lock lk{i_lock};
            int size = incoming_op_w.size();
            for (int i = 0 ; i < size; i++) {
                bool write = incoming_op_w[i];
                const T& d = incoming_data[i];
                if (write)
                    data.insert(d);
                else
                    data.erase(d);
            }
            incoming_op_w.clear();
            incoming_data.clear();
            return size;
        }

        bool insert(const T &item) {
            std::unique_lock lck(d_lock,std::defer_lock);
            bool inserted = false;
            if (!lck.try_lock()) {
                std::unique_lock lk{i_lock};
                incoming_op_w.push_back(true);
                incoming_data.push_back(item);
                inserted = true;
            } else {
                if (loaded) {
                    data.insert(item);
                    inserted = true;
                } else if (buffering) {
                    std::unique_lock lk{i_lock};
                    incoming_op_w.push_back(true);
                    incoming_data.push_back(item);
                    inserted = true;

                }
            }
            return inserted;
        }

        bool remove(const T &item) {
            std::unique_lock lck(d_lock,std::defer_lock);
            bool removed = false;
            if (!lck.try_lock()) {
                std::unique_lock lk(i_lock);
                incoming_op_w.push_back(false);
                incoming_data.push_back(item);
                removed = true;
            } else {
                if (loaded) {
                    data.erase(item);
                    removed = true;
                } else if (buffering) {
                    std::unique_lock lk(i_lock);
                    incoming_op_w.push_back(false);
                    incoming_data.push_back(item);
                    removed = true;
                }
            }
            return removed;
        }


        inline void lock() {
            d_lock.lock();
        }
        inline void unlock() {
            d_lock.unlock();
        }

        bool trim(bool force = false) {
            std::unique_lock lck(d_lock,std::defer_lock);
            if (!loaded) {
                return false;
            }
            if (lck.try_lock()) {
                if (force || ceph_clock_now() > ttl) {
                    loaded = false;
                    ttl = ceph_clock_now();
                    data.clear();
                    return true;
                }
            }

            return false;
        }
    };

    // poolid <-> pool data
    std::mutex pool_lock; //  = ceph::make_mutex("KvsStore::pool_lock");
    std::unordered_map<uint64_t, std::unordered_map<int8_t, LsData*> > cache;
    std::vector<LsData *> dirtylist;

    LsData *get_lsdata(int8_t shardid, uint64_t poolid, bool create) {
        std::unique_lock lock{pool_lock};
        auto &lsdata_list = cache[poolid];

        LsData *d = lsdata_list[shardid];
        if (d == 0 && create) {
            d = new LsData(cct, poolid, shardid);
            lsdata_list[shardid] = d;
            dirtylist.push_back(d);
        }

        return d;
    }

    inline bool param_check(struct iter_param &param, const T& t)
    {
        const uint32_t value = t.hobj.get_bitwise_key_u32();
        return value < param.endhash && value >= param.starthash;
    }

    inline bool range_check(const T& start, const T& end, const T& t)
    {
        return (t >= start && t < end);
    }
    CephContext *cct;

public:

    LsCache(CephContext *cct_) : cct(cct_) {

    }

    void trim(int i)
    {
        int deleted = 0;
        int max = (i == -1)? INT_MAX:i;
        std::unique_lock lock{pool_lock};

        auto it = dirtylist.begin();
        while (it != dirtylist.end()) {
            LsData *t = (*it);
            if (t->trim()) {
                dirtylist.erase(it);
                deleted++;
                if (deleted == max) break;
            }
            else {
                it++;
            }
        }
    }

    void clear()
    {
        std::unique_lock lock{pool_lock};
        for (const auto &t : cache) {
            for (const auto &p : t.second) {
                if (p.second) delete p.second;
            }
        }
        dirtylist.clear();
        cache.clear();
    }


    void load_and_search(const T& start, const T& end, int max, struct iter_param &temp, struct iter_param &other, DB *db, std::vector<T> *ls, T *pnext, bool destructive)
    {
        LsData *odatapage = (other.valid)? get_lsdata(other.shardid, other.poolid, true):0;
        LsData *tdatapage = (temp.valid)? get_lsdata(temp.shardid, temp.poolid, true):0;

        if (odatapage) {
            odatapage->fill(db, KEYSPACE_ONODE);
        }
        if (tdatapage) {
            tdatapage->fill(db, KEYSPACE_ONODE_TEMP);
        }

        if (odatapage) { odatapage->lock(); odatapage->apply_outstanding_requests();  }
        if (tdatapage) { tdatapage->lock(); tdatapage->apply_outstanding_requests();  }

        _search(odatapage, tdatapage, start, end, temp, other, max, ls, pnext);

        if (odatapage) { odatapage->unlock();  }
        if (tdatapage) { tdatapage->unlock();  }

        if (destructive) {
            if (odatapage) { odatapage->trim(true);  }
            if (tdatapage) { tdatapage->trim(true);  }
        }
    }



    int add(const T& t) {
        auto *page = get_lsdata(t.shard_id.id, t.hobj.pool + 0x8000000000000000ull, false);
        if (page) {
            if (page->insert(t)) {
                return 0;
            } else {
                return 1;
            }
        }
        return 2;
    }

    void remove(const T& t) {
        auto *page = get_lsdata(t.shard_id.id, t.hobj.pool + 0x8000000000000000ull, false);
        if (page) {

            page->remove(t);
        }
    }

    void _search(LsData *odatapage, LsData *tdatapage, const T& start, const T& end, struct iter_param &temp, struct iter_param &other, int max, std::vector<T> *ls, T *pnext) {
        //derr << "search " << odatapage << "," << tdatapage << " start " << start << ", end " << end  << ", max = " << max << dendl;

        std::set<ghobject_t>::iterator ofirst, olast, tfirst, tlast;
        if (odatapage == 0 && tdatapage == 0) {
            if (pnext) *pnext = ghobject_t::get_max();
            return;
        }


        ofirst = (odatapage)? odatapage->data.begin():tdatapage->data.end();
        olast  = (odatapage)? odatapage->data.end():tdatapage->data.end();
        tfirst = (tdatapage)? tdatapage->data.begin(): odatapage->data.end();
        tlast  = (tdatapage)? tdatapage->data.end(): odatapage->data.end();

        // merged iterator
        while (true) {
            const bool tfirst_done = (tfirst == tlast);
            const bool ofirst_done = (ofirst == olast);
            bool takeofirst;

            if (tfirst_done && ofirst_done) { //both are done
                break;
            }

            if (!tfirst_done && !ofirst_done) {
                takeofirst = (*ofirst < *tfirst);
            } else
                takeofirst = tfirst_done;

            if (takeofirst) {
                const T &t = *ofirst;
                if (range_check(start, end, t) && param_check(other ,t )) {
                    ls->push_back(t);
                    if (ls->size() == (unsigned)max) {
                        if (pnext) *pnext  = t;
                        return;
                    }
                }
                ++ofirst;
            } else {
                const T &t = *tfirst;
                if (range_check(start, end, t) && param_check(temp , t )) {
                    ls->push_back(t);
                    if (ls->size() == (unsigned)max) {
                        if (pnext) *pnext  = t;
                        return;
                    }
                }
                ++tfirst;
            }
        }

        if (pnext) *pnext = ghobject_t::get_max();

    }

};
#undef dout_context
#undef dout_subsys



#endif /* SRC_OS_KVSSTORE_KVSSTORE_INORDER_ITERATOR_H_ */
