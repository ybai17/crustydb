use crate::page::Offset;
use crate::page::Page;
use common::prelude::*;
use common::PAGE_SIZE;
use std::borrow::Borrow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, RwLock};

use std::io::BufWriter;
use std::io::{Seek, SeekFrom};

const SLOT_HEADER_SIZE: usize = 6;

//a wrapper for an u64 (specifically the file length)
// so that references can be made to it later
pub struct WrapperU64 {
    number: u64,
}

/// The struct for a heap file.  
///
/// HINT: You likely will want to design for interior mutability for concurrent accesses.
/// eg Arc<RwLock<>> on some internal members
///
/// HINT: You will probably not be able to serialize HeapFile, as it needs to maintain a link to a
/// File object, which cannot be serialized/deserialized/skipped by serde. You don't need to worry
/// about persisting read_count/write_count during serialization.
///
/// Your code should persist what information is needed to recreate the heapfile.
///
pub(crate) struct HeapFile {
    //TODO milestone hs
    //tracks page_id and maps them to their "physical" offset in the heap file
    //pub pages_and_physical_location: RefCell<HashMap<PageId, u32>>,
    pub pages_and_physical_location: Arc<RwLock<HashMap<PageId, Mutex<u32>>>>,

    //links page_id and the actual page
    //pub ids_to_pages_map: RefCell<HashMap<PageId, RefCell<Page>>>,
    pub ids_to_pages_map: Arc<RwLock<HashMap<PageId, RwLock<Page>>>>, 

    //number of BYTES in this heapfile (convert to number of pages by dividing by PAGE_SIZE)
    //pub heapfile_length: RefCell<WrapperU64>,
    pub heapfile_length: Arc<RwLock<WrapperU64>>,

    //the file object itself
    pub file: Arc<RwLock<File>>,

    // Track this HeapFile's container Id
    pub container_id: ContainerId,
    // The following are for profiling/ correctness checks
    pub read_count: AtomicU16,
    pub write_count: AtomicU16,
}

/// HeapFile required functions
impl HeapFile {
    /// Create a new heapfile for the given path. Return Result<Self> if able to create.
    /// Errors could arise from permissions, space, etc when trying to create the file used by HeapFile.
    pub(crate) fn new(file_path: PathBuf, container_id: ContainerId) -> Result<Self, CrustyError> {
        let mut file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)
        {
            Ok(f) => f,
            Err(error) => {
                return Err(CrustyError::CrustyError(format!(
                    "Cannot open or create heap file: {} {} {:?}",
                    file_path.to_string_lossy(),
                    error.to_string(),
                    error
                )))
            }
        };

        //TODO milestone hs
        let mut init_pages_and_physical_location: HashMap<PageId, Mutex<u32>> = HashMap::new();

        let init_heapfile_length: WrapperU64 = WrapperU64 {
            number: 0,
        };
         
		let len : u32 = file.seek(SeekFrom::End(0)).expect("Error when seeking to end of file") as u32;
		let mut offset : u32;
		let mut init_ids_to_pages_map : HashMap<PageId, RwLock<Page>> = HashMap::new();
		if len > 0 {			
			file.seek(SeekFrom::Start(0));
			offset = 0;
			let mut page_buffer : [u8; PAGE_SIZE] = [0; PAGE_SIZE];

			loop {
				file.read(&mut page_buffer);
				let page = Page::from_bytes(&page_buffer);
				let page_id = page.get_page_id();
				init_ids_to_pages_map.insert(page_id, RwLock::new(page));
				init_pages_and_physical_location.insert(page_id, Mutex::new(offset));
				offset += PAGE_SIZE as u32;
				if offset >= len {
					// Find all pages id
					break;
				}
			}
		}

        Ok(HeapFile {
            //TODO milestone hs
            pages_and_physical_location: Arc::new(RwLock::new(init_pages_and_physical_location)),

            ids_to_pages_map: Arc::new(RwLock::new(init_ids_to_pages_map)),

            //number of pages in this heapfile
            heapfile_length: Arc::new(RwLock::new(init_heapfile_length)),
        
            //the file object itself
            file: Arc::new(RwLock::new(file)),

            container_id,
            read_count: AtomicU16::new(0),
            write_count: AtomicU16::new(0),
        })
    }

    /// Return the number of pages for this HeapFile.
    /// Return type is PageId (alias for another type) as we cannot have more
    /// pages than PageId can hold.
    pub fn num_pages(&self) -> PageId {
        //return self.pages_and_physical_location.borrow().len() as PageId;
        return self.pages_and_physical_location.read().unwrap().len() as PageId;
    }

    /// Read the page from the file.
    /// Errors could arise from the filesystem or invalid pageId
    /// Note: that std::io::{Seek, SeekFrom} require Write locks on the underlying std::fs::File
    pub(crate) fn read_page_from_file(&self, pid: PageId) -> Result<Page, CrustyError> {
        //If profiling count reads
        #[cfg(feature = "profile")]
        {
            self.read_count.fetch_add(1, Ordering::Relaxed);
        }

        let page_offset_map = self.pages_and_physical_location.read().unwrap();

        if !page_offset_map.contains_key(&pid) {
            return Err(CrustyError::ValidationError("Invalid page ID".to_string()))
        }

        let page_offset = page_offset_map.get(&pid).unwrap();

        let mut read_buffer: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        
        let mut file_accessor = self.file.write().unwrap();
        file_accessor.seek(SeekFrom::Start(page_offset.lock().unwrap().clone() as u64));
        file_accessor.read(&mut read_buffer);

        let output_page: Page = Page::from_bytes(&read_buffer);

        let output_page_clone = Page::from_bytes(&output_page.to_bytes());

        self.ids_to_pages_map.write().unwrap().insert(output_page.get_page_id(), RwLock::new(output_page_clone));

        Ok(output_page)

    }

    /// Take a page and write it to the underlying file.
    /// This could be an existing page or a new page
    pub(crate) fn write_page_to_file(&self, page: Page) -> Result<(), CrustyError> {
        trace!(
            "Writing page {} to file {}",
            page.get_page_id(),
            self.container_id
        );
        //If profiling count writes
        #[cfg(feature = "profile")]
        {
            self.write_count.fetch_add(1, Ordering::Relaxed);
        }
        
        //check if new page or existing page
        let mut page_map = self.pages_and_physical_location.write().unwrap();

        let mut file_accessor = self.file.write().unwrap();

        if page_map.contains_key(&page.get_page_id()) {
            //existing page
            let offset = page_map.get(&page.get_page_id()).unwrap();
            file_accessor.seek(SeekFrom::Start(offset.lock().unwrap().clone() as u64));
        } else {
            //it's a new page
            
            //append file
            file_accessor.seek(SeekFrom::End(0));

            let mut heap_length = self.heapfile_length.write().unwrap();

            //update map
            page_map.insert(page.get_page_id(), Mutex::new(heap_length.number as u32));

            heap_length.number += PAGE_SIZE as u64;
        }

        // write file
        file_accessor.write(&page.to_bytes());

        Ok(())
    }

    //TODO: add find_first_available_page_id function, for use in insert_value(). Should return a page ID
    pub(crate) fn find_first_available_page_id(&self, size_to_check: usize) -> PageId {

        let mut page_ids_to_pages = self.ids_to_pages_map.write().unwrap();
        let page_ids_keys = page_ids_to_pages.keys().into_iter();
        let mut page_ids_only = Vec::from_iter(page_ids_keys);
        page_ids_only.sort();

        let mut last_offset = 0;

        //loop through all pages and check for the first available one
        for curr_page_id in page_ids_only.into_iter() {

            let curr_page = page_ids_to_pages.get(curr_page_id).unwrap();
            
            if curr_page.read().unwrap().get_free_space() < size_to_check + SLOT_HEADER_SIZE {
                //not enough space, continue
                continue;
            }

            //otherwise we have found the page with enough free space to write
            return *curr_page_id;
        }

        //if no available pages, then create a new one
        let new_page = Page::new(page_ids_to_pages.len() as PageId);

        let len = page_ids_to_pages.len();
        if  len > 0 {
			//last_offset = (*(self.pages_and_physical_location.borrow().get(&(len as u16 - 1)).unwrap())) as u32;
            last_offset = self.pages_and_physical_location.read().unwrap().get(&(len as u16 - 1)).unwrap().lock().unwrap().clone();
			last_offset += PAGE_SIZE as u32;
		}

        let new_page_id = new_page.get_page_id();

        //add new page to the HF data
        page_ids_to_pages.insert(new_page.get_page_id(), RwLock::new(new_page));
        self.pages_and_physical_location.write().unwrap().insert(new_page_id, Mutex::new(last_offset));

        return new_page_id;
    }

}

/*
impl Drop for HeapFile {
	fn drop(&mut self) {
		drop(&self.file);
	}
}
*/

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use common::testutil::*;
    use temp_testdir::TempDir;

    #[test]
    fn hs_hf_insert() {
        init();

        //Create a temp file
        let f = gen_random_test_sm_dir();
        let tdir = TempDir::new(f, true);
        let mut f = tdir.to_path_buf();
        f.push(gen_rand_string(4));
        f.set_extension("hf");

        let mut hf = HeapFile::new(f.to_path_buf(), 0).expect("Unable to create HF for test");

        // Make a page and write
        let mut p0 = Page::new(0);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let p0_bytes = p0.to_bytes();

        hf.write_page_to_file(p0);
        //check the page
        assert_eq!(1, hf.num_pages());
        let checkp0 = hf.read_page_from_file(0).unwrap();
        assert_eq!(p0_bytes, checkp0.to_bytes());

        //Add another page
        let mut p1 = Page::new(1);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let p1_bytes = p1.to_bytes();

        hf.write_page_to_file(p1);

        assert_eq!(2, hf.num_pages());
        //Recheck page0
        let checkp0 = hf.read_page_from_file(0).unwrap();
        assert_eq!(p0_bytes, checkp0.to_bytes());

        //check page 1
        let checkp1 = hf.read_page_from_file(1).unwrap();
        assert_eq!(p1_bytes, checkp1.to_bytes());

        #[cfg(feature = "profile")]
        {
            assert_eq!(*hf.read_count.get_mut(), 3);
            assert_eq!(*hf.write_count.get_mut(), 2);
        }
    }
}
