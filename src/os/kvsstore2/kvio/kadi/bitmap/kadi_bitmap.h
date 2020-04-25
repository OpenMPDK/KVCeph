/*
 * kad_bitmap.h
 *
 *  Created on: Nov 9, 2019
 *      Author: root
 */

#ifndef SRC_KADI_KADI_BITMAP_H_
#define SRC_KADI_KADI_BITMAP_H_

#include <stdint.h>
#include <stdio.h>
class fragment_manager {
	uint64_t *bitmap;
	int num_bitmaps;
	uint64_t assigned;
	uint64_t max_fragments;
public:

	fragment_manager(uint64_t *bitmap_, int num_bitmaps_):
		bitmap(bitmap_), num_bitmaps(num_bitmaps_), assigned(0), max_fragments(0) {}

	void init_new(int fragments) {
		max_fragments = fragments;
		for (int i = 0; i < num_bitmaps ; i++) {
			const int frag = (fragments < 64)? fragments:64;
			if (frag == 0) {
				bitmap[i] = 0;
			}
			else
				bitmap[i] = (uint64_t)(((__uint128_t)1 << frag) - 1);
			fragments -= frag;
		}
	}

	int assign_fragments(const int num_frags) {
		int offset = 0;
		uint64_t tbitmap;

		for (int i =0;i < 8; i++, offset += 64) {

			switch(num_frags) {
			case 1:
				tbitmap = bitmap[i];
				break;
			case 2:
				tbitmap = bitmap[i] & (bitmap[i] >> 1);
				break;

			case 3:
				tbitmap = bitmap[i] & (bitmap[i] >> 1) & (bitmap[i] >> 2);
				break;

			case 4:
				tbitmap = bitmap[i] & (bitmap[i] >> 1) & (bitmap[i] >> 2) & (bitmap[i] >> 3);
				break;
			}

			const int zeros = __builtin_ctzl( tbitmap );
			if (zeros == 64) continue;

			for (int j = 0; j < num_frags; j++) {
				bitmap[i] &= ~(1UL << (zeros + j));
			}

			//printf("assigned: key length = %d, num of fragments = %d\n", (int)length, num_frags);
			//print_bitmap(bitmap);
			assigned += num_frags;
			return offset + zeros;
		}

		return -1;
	}


	void return_fragments( int index, int num) {
		int bitmap_index = index / 64;
		int bitpos       = index % 64;
		for (int i =0 ; i < num ; i++)
			bitmap[bitmap_index] |= (1UL << (bitpos+i));
		assigned -= num;
	}

	inline bool empty() { return (assigned == 0);	}
	inline bool is_full() { return assigned == max_fragments; }

	void print_freespace() {
		for (int i = 0; i < num_bitmaps ; i++) {
			_print_bits(bitmap[i]);
		}
	}

	void _print_bits(uint64_t num){
	    uint64_t maxPow = ((uint64_t)1) << (64-1);
	    while(maxPow){
	        printf("%u", num&maxPow ? 1 : 0);
	        maxPow >>= 1;
	    }
	    printf("\n");
	}
};


#endif /* SRC_KADI_KADI_BITMAP_H_ */
