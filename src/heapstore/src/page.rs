use common::ids::{PageId, SlotId};
use common::PAGE_SIZE;
use core::panic;
use std::collections::{HashMap, LinkedList};
use std::convert::TryInto;
use std::fmt;

// Type to hold any value smaller than the size of a page.
// We choose u16 because it is sufficient to represent any slot that fits in a 4096-byte-sized page. 
// Note that you will need to cast Offset to usize if you want to use it to index an array.
pub type Offset = u16;
// For debug
const BYTES_PER_LINE: usize = 40;

const PAGE_HEADER_SIZE: usize = 8; //in bytes
const SLOT_HEADER_SIZE: usize = 6; //in bytes

#[derive(Copy, Clone)]
pub struct SlotHeader {
    slot_header_id: SlotId, // 2 bytes
    slot_size: u16, // includes the size of the header AKA SLOT_HEADER_SIZE
    used: u16, // 0 = empty, 1 = being used

    // tracks the location of the slot's used space within Page.data[]
    offset: usize,
}

#[derive(Copy, Clone)]
pub struct PageHeader {
    page_header_id: PageId, //2 bytes
    page_free_space: u16, //2 bytes
    max_slot_ids: u16, //2 bytes, a counter for unique slot Ids
}

/// Page struct. This must occupy not more than PAGE_SIZE when serialized.
/// In the header, you are allowed to allocate 8 bytes for general page metadata and
/// 6 bytes per value/entry/slot stored. For example a page that has stored 3 values, can use
/// up to 8+3*6=26 bytes, leaving the rest (PAGE_SIZE-26 for data) when serialized.
/// If you delete a value, you do not need reclaim header space the way you must reclaim page
/// body space. E.g., if you insert 3 values then delete 2 of them, your header can remain 26 
/// bytes & subsequent inserts can simply add 6 more bytes to the header as normal.
/// The rest must filled as much as possible to hold values.

pub(crate) struct Page {
    /// The data for data
    data: [u8; PAGE_SIZE],
    header: PageHeader,

    used_slots: HashMap<SlotId, SlotHeader>,
	blank_slots: Vec<SlotHeader>,
	available_slot_ids : Vec<SlotId>,
}

/// The functions required for page
impl Page {
    /// Create a new page
    pub fn new(page_id: PageId) -> Self {

        let output_header = PageHeader {
            page_header_id: page_id,
            page_free_space: (PAGE_SIZE - PAGE_HEADER_SIZE) as u16,
            max_slot_ids: 0,
        };

        let output_data: [u8; PAGE_SIZE] = [0; PAGE_SIZE];

        let mut output_slot_headers: Vec<SlotHeader> = Vec::new();

        let initial_empty_slot_header = SlotHeader {
            slot_header_id: 0,
            slot_size: (PAGE_SIZE - PAGE_HEADER_SIZE) as u16,
            used: 0,
            offset: PAGE_HEADER_SIZE,
        };

        output_slot_headers.push(initial_empty_slot_header);
		
		let mut blank_slots  = Vec::new();
		blank_slots.push(initial_empty_slot_header);

        let output_used_slots: HashMap<SlotId, SlotHeader> = HashMap::new();

        let output = Page {
            data: output_data,
            header: output_header,

            used_slots: output_used_slots,
			blank_slots : blank_slots,
			available_slot_ids : Vec::new(),
        };

        return output;
    }

    /// Return the page id for a page
    pub fn get_page_id(&self) -> PageId {
        return self.header.page_header_id;
    }


    /// Attempts to add a new value to this page if there is space available.
    /// Returns Some(SlotId) if it was inserted or None if there was not enough space.
    /// Note that where the bytes are stored in the page does not matter (heap), but it
    /// should not change the slotId for any existing value. This means that
    /// bytes in the page may not follow the slot order.
    /// If a slot is deleted you should replace the slotId on the next insert.
    ///
    /// HINT: You can copy/clone bytes into a slice using the following function.
    /// They must have the same size.
    /// self.data[X..y].clone_from_slice(&bytes);

    pub fn add_value(&mut self, bytes: &[u8]) -> Option<SlotId> {
		let size_needed : u16 = (bytes.len() + SLOT_HEADER_SIZE) as u16;
		if self.header.page_free_space < size_needed as u16 {
			return None;
		}

		// Find first available blank slot
		let mut found = false;
		let mut found_index = 0;
		for index in 0..self.blank_slots.len() {
			let slot = self.blank_slots[index];
			if slot.slot_size > size_needed {
				found = true;
				found_index = index;
				break;
			}
		}

		if !found {
			// Use first blank slot to merge and form a large enough blank slot
			found_index = 0;
			self.merge_blank_slots(found_index, size_needed as i32);
		}

		let slot = self.blank_slots[found_index];
		let new_slot_header_id;
		// Find the slot and slot id
		if self.available_slot_ids.len() == 0 {
			self.header.max_slot_ids += 1;
			new_slot_header_id = self.header.max_slot_ids;
		} else {
			// Reuse the slot id
			new_slot_header_id = self.available_slot_ids[0];
			self.available_slot_ids.remove(0);
		}

		// Split now
		// New slot used
		let new_slot = SlotHeader {
			slot_header_id : new_slot_header_id,
			slot_size : size_needed,
			used : 1,
			offset : slot.offset,
		};
		self.used_slots.insert(new_slot_header_id, new_slot);
		self.header.page_free_space -= size_needed;
		self.write_page_header();
				
		let start_pos = new_slot.offset + SLOT_HEADER_SIZE;
		let end_pos = new_slot.offset + size_needed as usize;
		self.data[start_pos..end_pos].clone_from_slice(&bytes);
		self.write_slot_header(new_slot);
	
		// Current blank slot reduce size, change offset
		self.blank_slots[found_index].slot_size -= size_needed;
		self.blank_slots[found_index].offset += size_needed as usize;

		if self.blank_slots[found_index].slot_size == 0 {
			// The slot is fully reused. Just delete from the blank vector
			self.blank_slots.remove(found_index);
			return Some(new_slot_header_id - 1);
		} else if self.blank_slots[found_index].slot_size > SLOT_HEADER_SIZE as u16 {
			// Enough space is for blank current slot
			self.write_slot_header(self.blank_slots[found_index]);
			return Some(new_slot_header_id - 1);
		}

		if self.blank_slots[found_index].offset >= PAGE_HEADER_SIZE - SLOT_HEADER_SIZE {
			// Current blank slot is the last one in all slots
			return Some(new_slot_header_id - 1);
		}
		// Have to merge this small unusable piece (cannot contain SlotHeader) with other blank ones
		// or move to the end of page
		self.merge_blank_slots(found_index, -1);
		return Some(new_slot_header_id - 1);
    }

	fn merge_blank_slots(&mut self, found_index : usize, expected_size : i32) {
		// just move all non-blank slots after me "slot_size" forward
		// till see another blank slot or reach the end the page

		let mut moved_distance = self.blank_slots[found_index].slot_size as usize;
		let mut move_start_pos : usize = self.blank_slots[found_index].offset + (self.blank_slots[found_index].slot_size as usize);
		let mut move_end_pos : usize = 0;
		let mut current_offset = move_start_pos;
	
		loop {
			if expected_size > 0 {
				if moved_distance >= expected_size as usize {
					// Done... got enough space
					return;
				}
			}
			loop {
				if current_offset > PAGE_SIZE - SLOT_HEADER_SIZE {
					// reach end of all slots
					break;
				}

				let mut moved_slot_id : SlotId = Page::get_word(&self.data, current_offset);
				let mut moved_slot_size = Page::get_word(&self.data, current_offset + 2);
				let mut moved_slot_used = Page::get_word(&self.data, current_offset + 4);
				if moved_slot_used == 0 {
					// Reach to a blank slot
					break;
				}

				// used slot...
				move_end_pos = current_offset + moved_slot_size as usize;
				// Adjust the offset of used slots
				let mut used_slot = self.used_slots.remove(&moved_slot_id).unwrap();
				used_slot.offset -= moved_distance;
				self.used_slots.insert(moved_slot_id, used_slot);

				current_offset = move_end_pos;
			}

			// Move the data
			let bytes = self.data[move_start_pos..move_end_pos].to_vec();
			let target_start_pos = move_start_pos-moved_distance;
			let target_end_pos = move_end_pos-moved_distance;
			self.data[target_start_pos..target_end_pos].clone_from_slice(&bytes);

			// set up current blank slot's correct offset
			self.blank_slots[found_index].offset = move_end_pos - moved_distance;

			if found_index < self.blank_slots.len() - 1 {
				// Merge two blank slots
				if self.blank_slots[found_index+1].used != 0 {
					panic!("Wrong with moved_slot_used case");
				}
				self.blank_slots[found_index].slot_size += self.blank_slots[found_index+1].slot_size;

				self.blank_slots.remove(found_index + 1);
			}

			if self.blank_slots[found_index].offset <= PAGE_SIZE - SLOT_HEADER_SIZE {
				self.write_slot_header(self.blank_slots[found_index]);
			}

			// Prepare for next round of move... the distance is size of current (merged) blank slot
			moved_distance = self.blank_slots[found_index].slot_size as usize;

			if expected_size < 0 {
				// Just run one round
				return;
			}
		}
	}


    // writes the page header's information to the page array so that it can be serialized
	fn write_page_header(&mut self) {
		self.set_word_to_position(self.header.page_header_id, 0);
		self.set_word_to_position(self.header.page_free_space as u16, 2);
		self.set_word_to_position(self.header.max_slot_ids, 4);
	}

    // writes the slot headers' information to the data for serialization
	fn write_slot_header(&mut self, slot : SlotHeader) {
		let loc = slot.offset;
		if loc > PAGE_SIZE - SLOT_HEADER_SIZE {
			panic!("Slot header beyond page {}", loc);
		}
		self.set_word_to_position(slot.slot_header_id, loc);
		if slot.slot_size == 0 {
			panic!("Slot size is 0!");
		}
		self.set_word_to_position(slot.slot_size as u16, loc+2);
		self.set_word_to_position(slot.used, loc+4);
	}

	/// Set the value to specified location
	fn set_word_to_position(&mut self, val : u16, loc : usize) {
		let val_array = val.to_le_bytes();
		self.data[loc] = val_array[0];
		self.data[loc+1] = val_array[1];
	}

	/// Get work as u16 from designated position
	fn get_word(data : &[u8], loc : usize) -> u16 {
		let mut ret_val: u16 = data[loc + 1] as u16;
		ret_val <<= 8;
		ret_val |= data[loc] as u16;
		
		return ret_val;
	}

    /// Return the bytes for the slotId. If the slotId is not valid then return None
    pub fn get_value(&self, slot_id: SlotId) -> Option<Vec<u8>> {
        let adjusted_id = slot_id + 1;

        if self.used_slots.contains_key(&adjusted_id) {

            //find starting point of the slot's data
            let data_start_point = (self.used_slots[&adjusted_id].offset as usize) + SLOT_HEADER_SIZE;

            //find end point of the slot data
            let data_end_point = data_start_point + (self.used_slots[&adjusted_id].slot_size as usize) - SLOT_HEADER_SIZE;

            let output = self.data[data_start_point..data_end_point].to_vec();

            Some(output)
        } else {
            None
        }
    }

    /// Delete the bytes/slot for the slotId. If the slotId is not valid then return None
    /// The slotId for a deleted slot should be assigned to the next added value
    /// The space for the value should be free to use for a later added value.
    /// HINT: Return Some(()) for a valid delete
    pub fn delete_value(&mut self, slot_id: SlotId) -> Option<()> {
        let adjusted_id = slot_id + 1;

        if self.used_slots.contains_key(&adjusted_id) {

            //update how much free space there is
            self.header.page_free_space += self.used_slots[&adjusted_id].slot_size;
			self.write_page_header();
            //println!("Current free space is {}", self.header.page_free_space);

            let mut removed_header = self.used_slots.remove(&adjusted_id).unwrap();
			
			let removed_offset = removed_header.offset;
			let removed_slotsize = removed_header.slot_size;

            removed_header.used = 0;
			self.write_slot_header(removed_header);

			self.blank_slots.push(removed_header);
			self.blank_slots.sort_by(|a, b| a.offset.cmp(&b.offset));
			self.available_slot_ids.push(removed_header.slot_header_id);
			self.available_slot_ids.sort();

			// Merge with previous/next if there are neighbour
			let mut index : usize = 0;
			let mut found = false;
			for index in 0..self.blank_slots.len() {
				if self.blank_slots[index].offset == removed_offset {
					found = true;
					break;
				}
			}

			if !found {
				panic!("Impossible! Should find the blank slot")
			}

			if index > 0 {
				// Try merge with previous neighbour
				if self.blank_slots[index-1].offset + self.blank_slots[index-1].slot_size as usize == removed_offset {
					self.blank_slots[index-1].slot_size += removed_slotsize;
					// Remove current slot as it merged with previous
					self.blank_slots.remove(index);
					self.write_slot_header(self.blank_slots[index-1]);
					index -= 1;
				}
			}

			if index < self.blank_slots.len() - 1 {
				// Try merge with next neighbour
				if self.blank_slots[index].offset + self.blank_slots[index].slot_size as usize == self.blank_slots[index+1].offset {
					self.blank_slots[index].slot_size += self.blank_slots[index+1].slot_size;
					// Remove next slot as it merged with current
					self.blank_slots.remove(index+1);
					self.write_slot_header(self.blank_slots[index]);
				}
			}
		}	
		Some(())
    }

    /// Deserialize bytes into Page
    ///
    /// HINT to create a primitive data type from a slice you can use the following
    /// (the example is for a u16 type and the data store in little endian)
    /// u16::from_le_bytes(data[X..Y].try_into().unwrap());
    pub fn from_bytes(data: &[u8]) -> Self {

        let mut output_page_header: PageHeader = PageHeader {
            page_header_id: 0, //2 bytes
            page_free_space: 0, //2 bytes
            max_slot_ids: 0, //2 bytes, a counter for unique slot Ids
        };

        let mut output_used_slots: HashMap<SlotId, SlotHeader> = HashMap::new();

        // Get page header information
        output_page_header.page_header_id = Page::get_word(&data, 0);
        output_page_header.page_free_space = Page::get_word(&data, 2);
        output_page_header.max_slot_ids = Page::get_word(&data, 4);

        //extract the slots
        let mut curr_slot_id: u16;
        let mut curr_slot_size: u16;
        let mut curr_slot_used: u16;
        let mut curr_slot_offset: u16;

        let mut curr_slot: SlotHeader;

        let mut curr_byte_index = PAGE_HEADER_SIZE;

		let mut output_blank_slots = Vec::new();

        //then loop through and extract the rest
        loop {
            if curr_byte_index >= PAGE_SIZE {
                break;
            }

            //when we reach the last unused bytes of the page, i.e. when there are <= 6 bytes left
            //assign it an empty slot in the slot header tracker of the page, in case space is freed up later and needs
            // merging
            if curr_byte_index >= (PAGE_SIZE - SLOT_HEADER_SIZE) {
                
                let curr_slot = SlotHeader {
                    slot_header_id: 0,
                    slot_size: (PAGE_SIZE - curr_byte_index) as u16,
                    used: 0,
                    offset: curr_byte_index,
                };

				output_blank_slots.push(curr_slot);
                break;
            }

            //extract slot header
            curr_slot_id = Page::get_word(&data, curr_byte_index);
            curr_slot_size = Page::get_word(&data, curr_byte_index+2);
            curr_slot_used = Page::get_word(&data, curr_byte_index+4);
            curr_slot_offset = curr_byte_index as u16;

            curr_slot = SlotHeader {
                slot_header_id: curr_slot_id,
                slot_size: curr_slot_size,
                used: curr_slot_used,
                offset: curr_slot_offset as usize,
            };

            if curr_slot_used == 1 {
                output_used_slots.insert(curr_slot_id, curr_slot);
            } else {
				output_blank_slots.push(curr_slot);
			}

            curr_byte_index += curr_slot_size as usize;
        }

		let mut output_available_slot_ids : Vec<u16> = Vec::new();
		for index in 1..=output_page_header.max_slot_ids {
			if ! output_used_slots.contains_key(&index) {
				output_available_slot_ids.push(index);
			}
		}

        //page struct to become the output
        let mut output_page: Page = Page {
            data: [0; PAGE_SIZE],
            header: output_page_header,

            used_slots: output_used_slots,
            blank_slots : output_blank_slots,
			available_slot_ids : output_available_slot_ids,
        };

        output_page.data[0..PAGE_SIZE].clone_from_slice(data);

        return output_page;
    }

    /// Serialize page into a byte array. This must be same size as PAGE_SIZE.
    /// We use a Vec<u8> for simplicity here.
    ///
    /// HINT: To convert a vec of bytes using little endian, use
    /// to_le_bytes().to_vec()
    /// HINT: Do not use the self debug ({:?}) in this function, which calls this function.
    pub fn to_bytes(&self) -> Vec<u8> {        
		return self.data.to_vec();
    }

    /// A utility function to determine the size of the header in the page
    /// when serialized/to_bytes.
    /// Will be used by tests. Optional for you to use in your code
    #[allow(dead_code)]
    pub(crate) fn get_header_size(&self) -> usize {
        return PAGE_HEADER_SIZE + self.used_slots.len() * SLOT_HEADER_SIZE;
    }

    /// A utility function to determine the total current free space in the page.
    /// This should account for the header space used and space that could be reclaimed if needed.
    /// Will be used by tests. Optional for you to use in your code, but strongly suggested
    #[allow(dead_code)]
    pub(crate) fn get_free_space(&self) -> usize {
        //println!("ASDASDASDASDASDASASADAS {}", self.header.pageFreeSpace);

        return self.header.page_free_space as usize;
    }

    /// utility function for determining the largest block of data free in the page
    pub(crate) fn get_largest_free_contiguous_space(&self) -> usize {
        panic!("TODO milestone pg");
    }

    /// Utility function for comparing the bytes of another page.
    /// Returns a vec  of Offset and byte diff
    #[allow(dead_code)]
    pub fn compare_page(&self, other_page: Vec<u8>) -> Vec<(Offset, Vec<u8>)> {
        let mut res = Vec::new();
        let bytes = self.to_bytes();
        assert_eq!(bytes.len(), other_page.len());
        let mut in_diff = false;
        let mut diff_start = 0;
        let mut diff_vec: Vec<u8> = Vec::new();
        for (i, (b1,b2)) in bytes.iter().zip(&other_page).enumerate(){
            if b1 != b2 {
                if !in_diff {
                    diff_start = i;
                    in_diff = true;
                }
                diff_vec.push(*b1);
            } else {
                if in_diff {
                    //end the diff
                    res.push((diff_start as Offset, diff_vec.clone()));
                    diff_vec.clear();
                    in_diff = false;
                }
            }
        }
        res
    }
}



pub struct PageIntoIter {
    //TODO milestone pg
    curr_page: Page,
    slots: Vec<SlotHeader>,
    curr_slot_index: u16,
}


/// The implementation of the (consuming) page iterator.
/// This should return the values in slotId order (ascending)
impl Iterator for PageIntoIter {
    // Each item returned by the iterator is the bytes for the value and the slot id.
    type Item = (Vec<u8>, SlotId);

    fn next(&mut self) -> Option<Self::Item> {

        if self.curr_slot_index < self.slots.len() as u16 {
            //go to curr_slot's offset in the data

            let curr_slot_id = self.slots[self.curr_slot_index as usize].slot_header_id;

            let curr_slot_offset = self.slots[self.curr_slot_index as usize].offset;
            let curr_slot_size = self.slots[self.curr_slot_index as usize].slot_size;

            let output_data = (self.curr_page.data[(curr_slot_offset + SLOT_HEADER_SIZE)..
                                                            (curr_slot_offset + curr_slot_size as usize)])
                                                            .to_vec();

            let output: Self::Item = (output_data, curr_slot_id - 1);

            //println!("Inside iterator.netx(){:?}",
            //self.curr_page.data[(curr_slot_offset + SLOT_HEADER_SIZE)..
            //                    (curr_slot_offset + curr_slot_size as usize)]
            //                    .to_vec());

            self.curr_slot_index += 1;

            Some(output)
        } else {  
            None
        }
    }
}


/// The implementation of IntoIterator which allows an iterator to be created
/// for a page. This should create the PageIter struct with the appropriate state/metadata
/// on initialization.
impl IntoIterator for Page {
    type Item = (Vec<u8>, SlotId);
    type IntoIter = PageIntoIter;

    fn into_iter(self) -> Self::IntoIter {

        let mut used_slot_ids = Vec::new();

        used_slot_ids = Vec::from_iter(self.used_slots.keys().into_iter());

        used_slot_ids.sort();

        let mut used_slot_headers = Vec::new();

        for curr_slot_id in used_slot_ids {
            used_slot_headers.push(self.used_slots[curr_slot_id]);
        }

        let page_iter = PageIntoIter {
            curr_page: self,
            slots: used_slot_headers,
            curr_slot_index: 0,
        };

        return page_iter;
    }
}

impl fmt::Debug for Page {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        //let bytes: &[u8] = unsafe { any_as_u8_slice(&self) };
        let p = self.to_bytes();
        let mut buffer = "".to_string();
        let len_bytes = p.len();

        // If you want to add a special header debugger to appear before the bytes add it here
        // buffer += "Header: \n";

        let mut pos = 0;
        let mut remaining;
        let mut empty_lines_count = 0;
        //: u8 = bytes_per_line.try_into().unwrap();
        let comp = [0; BYTES_PER_LINE];
        //hide the empty lines
        while pos < len_bytes {
            remaining = len_bytes - pos;
            if remaining > BYTES_PER_LINE {
                let pv = &(p)[pos..pos + BYTES_PER_LINE];
                if pv.eq(&comp) {
                    empty_lines_count += 1;
                    pos += BYTES_PER_LINE;
                    continue;
                }
                if empty_lines_count != 0 {
                    buffer += &format!("{} ", empty_lines_count);
                    buffer += "empty lines were hidden\n";
                    empty_lines_count = 0;
                }
                buffer += &format!("[0x{:08x}] ", pos);
                for i in 0..BYTES_PER_LINE {
                    match pv[i] {
                        0x00 => buffer += ".  ",
                        0xff => buffer += "## ",
                        _ => buffer += &format!("{:02x} ", pv[i]),
                    };
                }
            } else {
                let pv = &(p.clone())[pos..pos + remaining];
                if pv.eq(&comp) {
                    empty_lines_count += 1;
                    //buffer += "empty\n";
                    //println!("working");
                    pos += BYTES_PER_LINE;
                    continue;
                }
                if empty_lines_count != 0 {
                    buffer += &format!("{} ", empty_lines_count);
                    buffer += "empty lines were hidden\n";
                    empty_lines_count = 0;
                }
                buffer += &format!("[0x{:08x}] ", pos);
                for i in 0..remaining {
                    match pv[i] {
                        0x00 => buffer += ".  ",
                        0xff => buffer += "## ",
                        _ => buffer += &format!("{:02x} ", pv[i]),
                    };
                }
            }
            buffer += "\n";
            pos += BYTES_PER_LINE;
        }
        if empty_lines_count != 0 {
            buffer += &format!("{} ", empty_lines_count);
            buffer += "empty lines were hidden\n";
        }
        write!(f, "{}", buffer)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;
    use common::testutil::init;
    use common::testutil::*;
    use common::Tuple;
    use rand::Rng;

    /// Limits how on how many bytes we can use for page metadata / header
    pub const FIXED_HEADER_SIZE: usize = 8;
    pub const HEADER_PER_VAL_SIZE: usize = 6;

    #[test]
    fn hs_page_create() {
        init();
        let p = Page::new(0);
        assert_eq!(0, p.get_page_id());
        assert_eq!(PAGE_SIZE - p.get_header_size(), p.get_free_space());
    }

    #[test]
    fn debug_page_insert() {
        init();
        let mut p = Page::new(0);
        let n = 20;
        let size = 20;
        let vals = get_ascending_vec_of_byte_vec_02x(n, size, size);
        for x in &vals {
            p.add_value(x);
        }
        println!("{:?}", p);
        assert_eq!(
            p.get_free_space(),
            PAGE_SIZE - p.get_header_size() - n * size
        );
    }

    #[test]
    fn hs_page_simple_insert() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        let byte_len = tuple_bytes.len();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        assert_eq!(
            PAGE_SIZE - byte_len - p.get_header_size(),
            p.get_free_space()
        );
        let tuple_bytes2 = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - byte_len - byte_len,
            p.get_free_space()
        );
    }

    #[test]
    fn hs_page_space() {
        init();
        let mut p = Page::new(0);
        let size = 10;
        let bytes = get_random_byte_vec(size);
        assert_eq!(10, bytes.len());
        assert_eq!(Some(0), p.add_value(&bytes));
        assert_eq!(PAGE_SIZE - p.get_header_size() - size, p.get_free_space());
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 2,
            p.get_free_space()
        );
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_free_space()
        );
    }

    #[test]
    fn hs_page_get_value() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        //Recheck
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        //Check that invalid slot gets None
        assert_eq!(None, p.get_value(2));
    }

    #[test]
    fn hs_page_header_size_small() {
        init();
        // Testing that the header is no more than 8 bytes for the header, and 6 bytes per value inserted
        let mut p = Page::new(0);
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE);
        let bytes = get_random_byte_vec(10);
        assert_eq!(Some(0), p.add_value(&bytes));
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + HEADER_PER_VAL_SIZE);
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(Some(3), p.add_value(&bytes));
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + HEADER_PER_VAL_SIZE * 4);
    }

    #[test]
    fn hs_page_header_size_full() {
        init();
        // Testing that the header is no more than 8 bytes for the header, and 6 bytes per value inserted
        let mut p = Page::new(0);
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE);
        let byte_size = 10;
        let bytes = get_random_byte_vec(byte_size);
        // how many vals can we hold with 8 bytes
        let num_vals: usize = (((PAGE_SIZE - FIXED_HEADER_SIZE) as f64
            / (byte_size + HEADER_PER_VAL_SIZE) as f64)
            .floor()) as usize;
        if PAGE_SIZE == 4096 && FIXED_HEADER_SIZE == 8 && HEADER_PER_VAL_SIZE == 6 {
            assert_eq!(255, num_vals);
        }
        for _ in 0..num_vals {
            p.add_value(&bytes);
        }
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + (num_vals * HEADER_PER_VAL_SIZE));
        assert!(
            p.get_free_space()
                >= PAGE_SIZE
                    - (byte_size * num_vals)
                    - FIXED_HEADER_SIZE
                    - (num_vals * HEADER_PER_VAL_SIZE)
        );
    }

    #[test]
    fn hs_page_no_space() {
        init();
        let mut p = Page::new(0);
        let size = PAGE_SIZE / 4;
        let bytes = get_random_byte_vec(size);
        assert_eq!(Some(0), p.add_value(&bytes));
        assert_eq!(PAGE_SIZE - p.get_header_size() - size, p.get_free_space());
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 2,
            p.get_free_space()
        );
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_free_space()
        );
        //Should reject here
        assert_eq!(None, p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_free_space()
        );
        // Take small amount of data
        let small_bytes = get_random_byte_vec(size / 4);
        assert_eq!(Some(3), p.add_value(&small_bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3 - small_bytes.len(),
            p.get_free_space()
        );
    }

    #[test]
    fn hs_page_simple_delete() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        //Delete slot 0
        assert_eq!(Some(()), p.delete_value(0));

        //Recheck slot 1
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        //Verify slot 0 is gone
        assert_eq!(None, p.get_value(0));

        //Check that invalid slot gets None
        assert_eq!(None, p.get_value(2));

        //Delete slot 1
        assert_eq!(Some(()), p.delete_value(1));

        //Verify slot 0 is gone
        assert_eq!(None, p.get_value(1));
    }

    #[test]
    fn hs_page_get_first_free_space() {
        init();
        let mut p = Page::new(0);

        let _b1 = get_random_byte_vec(100);
        let _b2 = get_random_byte_vec(50);
    }

    #[test]
    fn hs_page_delete_insert() {
        init();
        let mut p = Page::new(0);
        let tuple_bytes = get_random_byte_vec(20);
        let tuple_bytes2 = get_random_byte_vec(20);
        let tuple_bytes3 = get_random_byte_vec(20);
        let tuple_bytes4 = get_random_byte_vec(20);
        let tuple_bytes_big = get_random_byte_vec(40);
        let tuple_bytes_small1 = get_random_byte_vec(5);
        let tuple_bytes_small2 = get_random_byte_vec(5);

        //Add 3 values
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        let check_bytes = p.get_value(1).unwrap();
        assert_eq!(tuple_bytes2, check_bytes);
        assert_eq!(Some(2), p.add_value(&tuple_bytes3));

        let check_bytes = p.get_value(2).unwrap();
        assert_eq!(tuple_bytes3, check_bytes);

        //Delete slot 1
        assert_eq!(Some(()), p.delete_value(1));
        //Verify slot 1 is gone
        assert_eq!(None, p.get_value(1));

        let check_bytes = p.get_value(0).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        let check_bytes = p.get_value(2).unwrap();
        assert_eq!(tuple_bytes3, check_bytes);

        //Insert same bytes, should go to slot 1
        assert_eq!(Some(1), p.add_value(&tuple_bytes4));

        let check_bytes = p.get_value(1).unwrap();
        assert_eq!(tuple_bytes4, check_bytes);

        //Delete 0
        assert_eq!(Some(()), p.delete_value(0));

        //Insert big, should go to slot 0 with space later in free block
        assert_eq!(Some(0), p.add_value(&tuple_bytes_big));

        //Insert small, should go to 3
        assert_eq!(Some(3), p.add_value(&tuple_bytes_small1));

        //Insert small, should go to new
        assert_eq!(Some(4), p.add_value(&tuple_bytes_small2));
    }

    #[test]
    fn hs_page_size() {
        init();
        let mut p = Page::new(2);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));

        let page_bytes = p.to_bytes();
        assert_eq!(PAGE_SIZE, page_bytes.len());
    }

    #[test]
    fn hs_page_simple_byte_serialize() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        //Get bytes and create from bytes
        let bytes = p.to_bytes();
        let mut p2 = Page::from_bytes(&bytes);
        assert_eq!(0, p2.get_page_id());

        //Check reads
        let check_bytes2 = p2.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);
        let check_bytes = p2.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        //Add a new tuple to the new page
        let tuple3 = int_vec_to_tuple(vec![4, 3, 2]);
        let tuple_bytes3 = tuple3.to_bytes();
        assert_eq!(Some(2), p2.add_value(&tuple_bytes3));
        assert_eq!(tuple_bytes3, p2.get_value(2).unwrap());
        assert_eq!(tuple_bytes2, p2.get_value(1).unwrap());
        assert_eq!(tuple_bytes, p2.get_value(0).unwrap());
    }

    #[test]
    fn hs_page_iter() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 0, 1]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));

        let tuple2 = int_vec_to_tuple(vec![0, 0, 2]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        let tuple3 = int_vec_to_tuple(vec![0, 0, 3]);
        let tuple_bytes3 = serde_cbor::to_vec(&tuple3).unwrap();
        assert_eq!(Some(2), p.add_value(&tuple_bytes3));

        let tuple4 = int_vec_to_tuple(vec![0, 0, 4]);
        let tuple_bytes4 = serde_cbor::to_vec(&tuple4).unwrap();
        assert_eq!(Some(3), p.add_value(&tuple_bytes4));

        let tup_vec = vec![
            tuple_bytes.clone(),
            tuple_bytes2.clone(),
            tuple_bytes3.clone(),
            tuple_bytes4.clone(),
        ];
        let page_bytes = p.to_bytes();

        // Test iteration 1
        let mut iter = p.into_iter();

        //println!("Inside test(){:?}", tuple_bytes.clone());

        assert_eq!(Some((tuple_bytes.clone(), 0)), iter.next());
        assert_eq!(Some((tuple_bytes2.clone(), 1)), iter.next());
        assert_eq!(Some((tuple_bytes3.clone(), 2)), iter.next());
        assert_eq!(Some((tuple_bytes4.clone(), 3)), iter.next());
        assert_eq!(None, iter.next());

        //Check another way
        let p = Page::from_bytes(&page_bytes);
        assert_eq!(Some(tuple_bytes.clone()), p.get_value(0));

        for (i, x) in p.into_iter().enumerate() {
            assert_eq!(tup_vec[i], x.0);
        }

        let p = Page::from_bytes(&page_bytes);
        let mut count = 0;
        for _ in p {
            count += 1;
        }
        assert_eq!(count, 4);

        //Add a value and check
        let mut p = Page::from_bytes(&page_bytes);
        assert_eq!(Some(4), p.add_value(&tuple_bytes));
        //get the updated bytes
        let page_bytes = p.to_bytes();
        count = 0;
        for _ in p {
            count += 1;
        }
        assert_eq!(count, 5);

        //Delete
        let mut p = Page::from_bytes(&page_bytes);
        p.delete_value(2);
        let mut iter = p.into_iter();
        assert_eq!(Some((tuple_bytes.clone(), 0)), iter.next());
        assert_eq!(Some((tuple_bytes2.clone(), 1)), iter.next());
        assert_eq!(Some((tuple_bytes4.clone(), 3)), iter.next());
        assert_eq!(Some((tuple_bytes.clone(), 4)), iter.next());
        assert_eq!(None, iter.next());
    }

    #[test]
    pub fn hs_page_test_delete_reclaim_same_size() {
        init();
        let size = 800;
        let values = get_ascending_vec_of_byte_vec_02x(6, size, size);
        let mut p = Page::new(0);
        assert_eq!(Some(0), p.add_value(&values[0]));
        assert_eq!(Some(1), p.add_value(&values[1]));
        assert_eq!(Some(2), p.add_value(&values[2]));
        assert_eq!(Some(3), p.add_value(&values[3]));
        assert_eq!(Some(4), p.add_value(&values[4]));
        assert_eq!(values[0], p.get_value(0).unwrap());
        assert_eq!(None, p.add_value(&values[0]));
        assert_eq!(Some(()), p.delete_value(1));
        assert_eq!(None, p.get_value(1));
        assert_eq!(Some(1), p.add_value(&values[5]));
        assert_eq!(values[5], p.get_value(1).unwrap());
    }

    #[test]
    pub fn hs_page_test_delete_reclaim_larger_size() {
        init();
        let size = 500;
        let values = get_ascending_vec_of_byte_vec_02x(8, size, size);
        let larger_val = get_random_byte_vec(size * 2 - 20);
        let mut p = Page::new(0);
        assert_eq!(Some(0), p.add_value(&values[0]));
        assert_eq!(Some(1), p.add_value(&values[1]));
        assert_eq!(Some(2), p.add_value(&values[2]));
        assert_eq!(Some(3), p.add_value(&values[3]));
        assert_eq!(Some(4), p.add_value(&values[4]));
        assert_eq!(Some(5), p.add_value(&values[5]));
        assert_eq!(Some(6), p.add_value(&values[6]));
        assert_eq!(Some(7), p.add_value(&values[7]));
        assert_eq!(values[5], p.get_value(5).unwrap());
        assert_eq!(None, p.add_value(&values[0]));
        assert_eq!(Some(()), p.delete_value(1));
        assert_eq!(None, p.get_value(1));
        assert_eq!(Some(()), p.delete_value(6));
        assert_eq!(None, p.get_value(6));
        assert_eq!(Some(1), p.add_value(&larger_val));
        assert_eq!(larger_val, p.get_value(1).unwrap());
    }

    #[test]
    pub fn hs_page_test_delete_reclaim_smaller_size() {
        init();
        let size = 800;
        let values = vec![
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size / 4),
        ];
        let mut p = Page::new(0);
        assert_eq!(Some(0), p.add_value(&values[0]));
        assert_eq!(Some(1), p.add_value(&values[1]));
        assert_eq!(Some(2), p.add_value(&values[2]));
        assert_eq!(Some(3), p.add_value(&values[3]));
        assert_eq!(Some(4), p.add_value(&values[4]));
        assert_eq!(values[0], p.get_value(0).unwrap());
        assert_eq!(None, p.add_value(&values[0]));
        assert_eq!(Some(()), p.delete_value(1));
        assert_eq!(None, p.get_value(1));
        assert_eq!(Some(1), p.add_value(&values[5]));
        assert_eq!(values[5], p.get_value(1).unwrap());
    }

    #[test]
    pub fn hs_page_test_multi_ser() {
        init();
        let size = 500;
        let values = vec![
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
        ];
        let mut p = Page::new(0);
        assert_eq!(Some(0), p.add_value(&values[0]));
        assert_eq!(Some(1), p.add_value(&values[1]));
        assert_eq!(Some(2), p.add_value(&values[2]));
        let bytes = p.to_bytes();
        let mut p2 = Page::from_bytes(&bytes);
        assert_eq!(values[0], p2.get_value(0).unwrap());
        assert_eq!(values[1], p2.get_value(1).unwrap());
        assert_eq!(values[2], p2.get_value(2).unwrap());
        assert_eq!(Some(3), p2.add_value(&values[3]));
        assert_eq!(Some(4), p2.add_value(&values[4]));

        let bytes2 = p2.to_bytes();
        let mut p3 = Page::from_bytes(&bytes2);
        assert_eq!(values[0], p3.get_value(0).unwrap());
        assert_eq!(values[1], p3.get_value(1).unwrap());
        assert_eq!(values[2], p3.get_value(2).unwrap());
        assert_eq!(values[3], p3.get_value(3).unwrap());
        assert_eq!(values[4], p3.get_value(4).unwrap());
        assert_eq!(Some(5), p3.add_value(&values[5]));
        assert_eq!(Some(6), p3.add_value(&values[6]));
        assert_eq!(Some(7), p3.add_value(&values[7]));
        assert_eq!(None, p3.add_value(&values[0]));

        let bytes3 = p3.to_bytes();
        let p4 = Page::from_bytes(&bytes3);
        assert_eq!(values[0], p4.get_value(0).unwrap());
        assert_eq!(values[1], p4.get_value(1).unwrap());
        assert_eq!(values[2], p4.get_value(2).unwrap());
        assert_eq!(values[7], p4.get_value(7).unwrap());
    }

    #[test]
    pub fn hs_page_stress_test() {
        init();
        let mut p = Page::new(23);
        let mut original_vals: VecDeque<Vec<u8>> =
            VecDeque::from_iter(get_ascending_vec_of_byte_vec_02x(300, 20, 100));
        let mut stored_vals: Vec<Vec<u8>> = Vec::new();
        let mut stored_slots: Vec<SlotId> = Vec::new();
        let mut has_space = true;
        let mut rng = rand::thread_rng();
        // Load up page until full
        while has_space {
            let bytes = original_vals
                .pop_front()
                .expect("ran out of data -- shouldn't happen");
            let slot = p.add_value(&bytes);
            match slot {
                Some(slot_id) => {
                    stored_vals.push(bytes);
                    stored_slots.push(slot_id);
                }
                None => {
                    // No space for this record, we are done. go ahead and stop. add back value
                    original_vals.push_front(bytes);
                    has_space = false;
                }
            };
        }
        // let (check_vals, check_slots): (Vec<Vec<u8>>, Vec<SlotId>) = p.into_iter().map(|(a, b)| (a, b)).unzip();
        let bytes = p.to_bytes();
        let p_clone = Page::from_bytes(&bytes);
        let mut check_vals: Vec<Vec<u8>> = p_clone.into_iter().map(|(a, _)| a).collect();
        assert!(compare_unordered_byte_vecs(&stored_vals, check_vals));
        trace!("\n==================\n PAGE LOADED - now going to delete to make room as needed \n =======================");
        // Delete and add remaining values until goes through all. Should result in a lot of random deletes and adds.
        while !original_vals.is_empty() {
            let bytes = original_vals.pop_front().unwrap();
            trace!("Adding new value (left:{}). Need to make space for new record (len:{}).\n - Stored_slots {:?}", original_vals.len(), &bytes.len(), stored_slots);
            let mut added = false;
            while !added {
                let try_slot = p.add_value(&bytes);
                match try_slot {
                    Some(new_slot) => {
                        stored_slots.push(new_slot);
                        stored_vals.push(bytes.clone());
                        let bytes = p.to_bytes();
                        let p_clone = Page::from_bytes(&bytes);
                        check_vals = p_clone.into_iter().map(|(a, _)| a).collect();
                        assert!(compare_unordered_byte_vecs(&stored_vals, check_vals));
                        trace!("Added new value ({}) {:?}", new_slot, stored_slots);
                        added = true;
                    }
                    None => {
                        //Delete a random value and try again
                        let random_idx = rng.gen_range(0..stored_slots.len());
                        trace!(
                            "Deleting a random val to see if that makes enough space {}",
                            stored_slots[random_idx]
                        );
                        let value_id_to_del = stored_slots.remove(random_idx);
                        stored_vals.remove(random_idx);
                        p.delete_value(value_id_to_del)
                            .expect("Error deleting slot_id");
                        trace!("Stored vals left {}", stored_slots.len());
                    }
                }
            }
        }
    }

}
