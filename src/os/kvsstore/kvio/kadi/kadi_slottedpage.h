/*
 * kadi_slottedpage.h
 *
 * Slotted Page implementation:
 *   borrowed from github.com/julienschmidt/moderndbs (DBMS course material)
 */

#ifndef SRC_KADI_KADI_SLOTTEDPAGE_H_
#define SRC_KADI_KADI_SLOTTEDPAGE_H_

#include "kadi_nodepool.h"
#include <queue>

class KvsSlottedPage : public kv_indexnode {
	struct Header {
		uint16_t slotCount;     // number of used slots
		uint16_t firstFreeSlot; // cache to speed up locating free slots
		uint16_t dataStart;     // lower end of the data
		uint16_t freeSpace;     // space that would be available after compaction
	};

	struct Slot {
		uint16_t offset; // start of the data item
		uint8_t  length; // length of the data item

		inline bool is_free() { return (offset == 0); }
	};
	struct Header *header;

public:

	KvsSlottedPage(const bp_addr_t &addr, char *buffer, int buffer_size, bool isnew):
		kv_indexnode(addr, buffer, buffer_size), header((struct Header *) buffer)
	{
		if (isnew) {
	        header->slotCount     = 0;
	        header->firstFreeSlot = 0;
			header->dataStart     = buffer_size;
			header->freeSpace     = buffer_size-sizeof(Header);
			//std::cout << "init: freespace = " << header->freeSpace << std::endl;
		}
	};

	bp_addr_t insert(const char *key, uint8_t length) {
		int slotID;
		const unsigned space_required = length + sizeof (Slot);
		if (space_required > header->freeSpace) return invalid_key_addr;

		uint16_t headEnd   = sizeof(Header) + (header->slotCount+1)*sizeof(Slot);
		uint16_t dataStart = header->dataStart;

		if (dataStart <= headEnd || length > (dataStart - headEnd)) {
			//must compact the page
			compactPage();
		}

		uint32_t slotCount = header->slotCount;

		// look for a existing free slot
		if (header->firstFreeSlot < slotCount) {
			slotID = header->firstFreeSlot;

			// search for the next free slot
			uint32_t i = slotID+1;
			for(; i < slotCount; i++) {
				Slot& otherSlot = reinterpret_cast<Slot*>(buffer+sizeof(Header))[i];
				if (otherSlot.is_free())
					break;
			}
			header->firstFreeSlot = i;

			// update free space
			header->freeSpace -= length;

		// insert new Slot
		} else {
			slotID = slotCount;

			header->slotCount++;
			header->firstFreeSlot = header->slotCount;

			// update free space
			header->freeSpace -= sizeof(Slot) + length;
		}

		Slot *slot = (Slot*)(buffer + sizeof(Header) + slotID*sizeof(Slot));

		//std::cout << "data start = " << header->dataStart << ", freespace = " << header->freeSpace << std::endl;

		// Insert Record Data
	    header->dataStart -= length;
	    slot->offset = header->dataStart;
	    slot->length = length;
	    memcpy(buffer + slot->offset, key, length);


		return create_key_addr(bpaddr_pageid(addr), slotID);
	}

    bool remove(bp_addr_t addr)
    {
    	if (addr == invalid_key_addr) return false;

    	unsigned slotID = bpaddr_fragid(addr);

		Header& header = reinterpret_cast<Header*>(buffer)[0];
		Slot&   slot   = reinterpret_cast<Slot*>(buffer+sizeof(Header))[slotID];

		// update the first free slot cache
		if (slotID < header.firstFreeSlot)
			header.firstFreeSlot = slotID;

		// if this slots contains the last data block, adjust header->dataStart
		if (slot.offset == header.dataStart)
			header.dataStart += slot.length;

		// update free space
		header.freeSpace += slot.length;
		//std::cout << "remove, freespace = " << header.freeSpace << std::endl;
		// mark this slot as empty
		slot.length = 0;
		slot.offset = 0;

		return true;
    }

    // Returns the read-only record associated with TID tid
    bool lookup(bp_addr_t addr, char **data, int &length) {
    	if (addr == invalid_key_addr) return false;

		Slot& slot = reinterpret_cast<Slot*>(buffer+sizeof(Header))[bpaddr_fragid(addr)];
		*data = buffer+slot.offset;
		length = slot.length;
		return true;
    }

    // compacts the given page by moving records
    void compactPage() {
	 	// open the page for writings


		Header*      header = reinterpret_cast<Header*>(buffer);

		uint32_t slotCount = header->slotCount;
		Slot*    slots     = reinterpret_cast<Slot*>(buffer+sizeof(Header));

		// put all non-free and non-indirection slots in a priority queue
		struct SlotPtrCmpOffst {
			bool operator() (Slot* const &a, Slot* const &b) {
				return a->offset < b->offset;
			}
		};

		std::priority_queue<Slot*, std::vector<Slot*>, SlotPtrCmpOffst> slotPtrs;
		for(uint32_t slotID = 0; slotID < slotCount; slotID++) {
			Slot& slot = slots[slotID];

			if (!slot.is_free())
				slotPtrs.push(&slot);
		}

		off_t offset = buffer_size;

		// move records
		while (!slotPtrs.empty()) {
			Slot* slot = slotPtrs.top();
			slotPtrs.pop();

			// nothing to move, if the record length is 0 bytes
			if (slot->length == 0)
				continue;

			// move the data
			offset -= slot->length;
			memmove(buffer+offset, buffer+slot->offset, slot->length);
			slot->offset = offset;
		}

		header->dataStart = offset;


    }

};



#endif /* SRC_KADI_KADI_SLOTTEDPAGE_H_ */
