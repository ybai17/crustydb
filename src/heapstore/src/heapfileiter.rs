use crate::heapfile::HeapFile;
use crate::page::{PageIntoIter, Page};
use common::prelude::*;
use std::borrow::Borrow;
use std::sync::Arc;

#[allow(dead_code)]
/// The struct for a HeapFileIterator.
/// We use a slightly different approach for HeapFileIterator than
/// standard way of Rust's IntoIter for simplicity (avoiding lifetime issues).
/// This should store the state/metadata required to iterate through the file.
///
/// HINT: This will need an Arc<HeapFile>
pub struct HeapFileIterator {
    //TODO milestone hs
    data: Vec<(Vec<u8>, PageId, SlotId)>,
    curr_hf_index: usize,
    container_id: ContainerId,
}

/// Required HeapFileIterator functions
impl HeapFileIterator {
    /// Create a new HeapFileIterator that stores the tid, and heapFile pointer.
    /// This should initialize the state required to iterate through the heap file.
    pub(crate) fn new(container_id: ContainerId, tid: TransactionId, hf: Arc<&HeapFile>) -> Self {
        let mut output = HeapFileIterator {
            data: Vec::new(),
            curr_hf_index: 0,
            container_id: container_id,
        };

        let ids = hf.ids_to_pages_map.read().unwrap();
        let ids_keys = ids.keys().into_iter();
        let page_ids = Vec::from_iter(ids_keys);

        let mut sorted_ids = page_ids.clone();

        sorted_ids.sort();

        for curr_id in sorted_ids {
            let curr_page = ids.borrow().get(curr_id).unwrap();
            let curr_page_id = curr_page.read().unwrap().get_page_id();

            let page_iterator = Page::from_bytes(&curr_page.read().unwrap().to_bytes().clone());

            //loop through the page itself
            for curr_page_slot in page_iterator.into_iter() {
                
                let output_tuple = (curr_page_slot.0.clone(), curr_page_id, curr_page_slot.1);
				//println!("Current page id {} slot id {}", curr_page_id, curr_page_slot.1);

                output.data.push(output_tuple);
            }
        }

        return output;
    }
}

/// Trait implementation for heap file iterator.
/// Note this will need to iterate through the pages and their respective iterators.
impl Iterator for HeapFileIterator {
    type Item = (Vec<u8>, ValueId);
    fn next(&mut self) -> Option<Self::Item> {
        
        if self.curr_hf_index >= self.data.len() {
            None
        } else {
            let iterator_data = &self.data[self.curr_hf_index];

            let iterator_data_0 = iterator_data.0.clone();

            let output_value_id = ValueId {
                container_id: self.container_id,
                segment_id: None,
                page_id: Some(iterator_data.1), 
                slot_id: Some(iterator_data.2),
            };
            self.curr_hf_index += 1;

            Some((iterator_data_0, output_value_id))
        }

    }
}
