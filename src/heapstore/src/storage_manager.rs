use crate::heapfile::HeapFile;
use crate::heapfileiter::HeapFileIterator;
use crate::page::Page;
use common::delta_storage_trait::Value;
use common::prelude::*;
use common::storage_trait::StorageTrait;
use common::testutil::{gen_random_test_sm_dir, gen_rand_string};
use common::PAGE_SIZE;
use temp_testdir::TempDir;
use std::borrow::{Borrow, BorrowMut};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::hash::Hash;
use std::io::{Write, BufReader, BufWriter};
use std::path::{PathBuf, Path};
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use std::thread::current;


/// The StorageManager struct
#[derive(Serialize, Deserialize)]
pub struct StorageManager {
    /// Path to database metadata files.
    pub storage_path: PathBuf,
    /// Indicates if this is a temp StorageManager (for testing)
    is_temp: bool,

    //map of container IDs to PathBufs, to be serde'd
    //pub id_to_filepath_map: RefCell<HashMap<ContainerId, PathBuf>>,
    pub id_to_filepath_map: Arc<RwLock<HashMap<ContainerId, PathBuf>>>,

    //map of container IDs directly to their associated HeapFile
    #[serde(skip)]
    //container_to_hf_map: RefCell<HashMap<ContainerId, HeapFile>>,
    container_to_hf_map: Arc<RwLock<HashMap<ContainerId, HeapFile>>>,
}

/// The required functions in HeapStore's StorageManager that are specific for HeapFiles
impl StorageManager {
    /// Get a page if exists for a given container.
    pub(crate) fn get_page(
        &self,
        container_id: ContainerId,
        page_id: PageId,
        _tid: TransactionId,
        _perm: Permissions,
        _pin: bool,
    ) -> Option<Page> {
        
        let container_hf_map = self.container_to_hf_map.read().unwrap();

        //check if valid container ID
        if container_hf_map.contains_key(&container_id) {

            //check if valid page ID
            
            let current_hf = container_hf_map.get(&container_id).unwrap();

            if current_hf.pages_and_physical_location.read().unwrap().contains_key(&page_id) {

                let borrowed_page = current_hf.ids_to_pages_map.read().unwrap();

                let unwrapped_page = borrowed_page.get(&page_id).unwrap();

                let output_page = Page::from_bytes(&unwrapped_page.read().unwrap().to_bytes());

                Some(output_page)

            } else {
                None
            }
        } else {
            None
        }
    }

    /// Write a page
    pub(crate) fn write_page(
        &self,
        container_id: ContainerId,
        page: Page,
        _tid: TransactionId,
    ) -> Result<(), CrustyError> {

        let container_hf_map = self.container_to_hf_map.read().unwrap();

        //check if valid container ID
        if container_hf_map.contains_key(&container_id) {

            let current_hf = container_hf_map.borrow().get(&container_id).unwrap();

            current_hf.write_page_to_file(page)
        } else {
            Err(CrustyError::ValidationError("Invalid container_id".to_string()))
        }
    }

    /// Get the number of pages for a container
    fn get_num_pages(&self, container_id: ContainerId) -> PageId {
        if !self.id_to_filepath_map.read().unwrap().contains_key(&container_id) {
            panic!("Invalid ID");
        } else {
            return self.container_to_hf_map.read().unwrap().get(&container_id).unwrap().num_pages();
        }
    }

    /// Test utility function for counting reads and writes served by the heap file.
    /// Can return 0,0 for invalid container_ids
    #[allow(dead_code)]
    pub(crate) fn get_hf_read_write_count(&self, container_id: ContainerId) -> (u16, u16) {
        if !self.id_to_filepath_map.read().unwrap().contains_key(&container_id) {
            return (0, 0);
        } else {
            return (self.container_to_hf_map.read().unwrap().get(&container_id).unwrap().read_count.load(Ordering::Relaxed),
                    self.container_to_hf_map.read().unwrap().get(&container_id).unwrap().write_count.load(Ordering::Relaxed));
        }
    }

    /// For testing
    pub fn get_page_debug(&self, container_id: ContainerId, page_id: PageId) -> String {
        match self.get_page(container_id, page_id, TransactionId::new(), Permissions::ReadOnly, false) {
            Some(p) => {
                format!("{:?}",p)
            },
            None => String::new()
        }
    }

    /// For testing
    pub fn get_page_bytes(&self, container_id: ContainerId, page_id: PageId) -> Vec<u8> {
        match self.get_page(container_id, page_id, TransactionId::new(), Permissions::ReadOnly, false) {
            Some(p) => {
                p.to_bytes()
            },
            None => Vec::new()
        }
    }

    
}

/// Implementation of storage trait
impl StorageTrait for StorageManager {
    type ValIterator = HeapFileIterator;

    /// Create a new storage manager that will use storage_path as the location to persist data
    /// (if the storage manager persists records on disk; not the case for memstore)
    /// For startup/shutdown: check the storage_path for data persisted in shutdown() that you can
    /// use to populate this instance of the SM. Otherwise create a new one.
    fn new(storage_path: PathBuf) -> Self {

        //first need to check the file
		let mut sm_meta = storage_path.clone();
		sm_meta.push("sm.mta");
        let mut file_result = File::open(&sm_meta);
		match file_result {
			Ok(f) => {
				let mut reader = BufReader::new(f);
				let mut sm : StorageManager = serde_json::from_reader(reader).expect("Failed to deserialize StorageManager");
				let container_heapfile_map : HashMap<ContainerId, HeapFile> = HashMap::new();
				sm.container_to_hf_map = Arc::new(RwLock::new(container_heapfile_map));		
                
				return sm;
			},
			Err(e) => {
                let container_path_map : HashMap<ContainerId, PathBuf> = HashMap::new();
		        let container_heapfile_map : HashMap<ContainerId, HeapFile> = HashMap::new();
				fs::create_dir_all(&storage_path);
                let sm = Self {
                    storage_path : storage_path,
                    is_temp : false,
                    id_to_filepath_map : Arc::new(RwLock::new(container_path_map)),
                    container_to_hf_map : Arc::new(RwLock::new(container_heapfile_map)),
                };
                return sm;
            },
		};
    }

    /// Create a new storage manager for testing. There is no startup/shutdown logic here: it
    /// should simply create a fresh SM and set is_temp to true
    fn new_test_sm() -> Self {
        let storage_path = gen_random_test_sm_dir();
        debug!("Making new temp storage_manager {:?}", storage_path);
        
        let mut output_file_map: Arc<RwLock<HashMap<ContainerId, PathBuf>>> = Arc::new(RwLock::new(HashMap::new()));

        let mut output_container_map: Arc<RwLock<HashMap<ContainerId, HeapFile>>> = Arc::new(RwLock::new(HashMap::new()));

        let output: StorageManager = StorageManager {
            storage_path: storage_path,
            is_temp: true,
            id_to_filepath_map: output_file_map,
            container_to_hf_map: output_container_map,
        };

        return output;
    }

    fn get_simple_config() -> common::ContainerConfig {
        common::ContainerConfig::simple_container()
    }

    // helper function for insert_value()
    // returns the page_id of the first available page that can fit the data to be stored

    /// Insert some bytes into a container for a particular value (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns the value id associated with the stored value.
    /// Function will need to find the first page that can hold the value.
    /// A new page may need to be created if no space on existing pages can be found.
    fn insert_value(
        &self,
        container_id: ContainerId,
        value: Vec<u8>,
        tid: TransactionId,
    ) -> ValueId {
        if value.len() > PAGE_SIZE {
            panic!("Cannot handle inserting a value larger than the page size");
        }
        
        let mut output = ValueId {
            container_id: container_id,
            segment_id: None,
            page_id: None,
            slot_id: None,
        };

        let mut container_to_hf_map = self.container_to_hf_map.write().unwrap();

        //first check for valid container
        if !container_to_hf_map.contains_key(&container_id) {
            if !self.id_to_filepath_map.read().unwrap().contains_key(&container_id) {
                panic!("insert_value(): Invalid container ID")
            }

            //create new heapfile path and load in the heapfile
            let container_path_map = self.id_to_filepath_map.read().unwrap();
            let filepath = container_path_map.get(&container_id).unwrap();

            let heapfile = HeapFile::new((*filepath).clone(), container_id).unwrap();

            container_to_hf_map.insert(container_id, heapfile);
        }

		let mut ref_heap_file = container_to_hf_map.get(&container_id).unwrap();
		let mut heap_file = ref_heap_file.borrow_mut();
		let page_id = heap_file.find_first_available_page_id(value.len() as usize);
        
        let mut hf_ids_to_page_map = heap_file.ids_to_pages_map.read().unwrap();
        let mut temp_page = hf_ids_to_page_map.get(&page_id).unwrap();

		let slot_id = temp_page.write().unwrap().add_value(&value);
		match slot_id {
			Some(s) => output.slot_id = Some(s),
			None => panic!("couldn't find slot available!"),
		}

		output.page_id = Some(page_id);

        return output;
    }

    /// Insert some bytes into a container for vector of values (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns a vector of value ids associated with the stored values.
    fn insert_values(
        &self,
        container_id: ContainerId,
        values: Vec<Vec<u8>>,
        tid: TransactionId,
    ) -> Vec<ValueId> {
        let mut ret = Vec::new();
        for v in values {
            ret.push(self.insert_value(container_id, v, tid));
        }
        ret
    }

    /// Delete the data for a value. If the valueID is not found it returns Ok() still.
    fn delete_value(&self, id: ValueId, tid: TransactionId) -> Result<(), CrustyError> {

        let container_id = id.container_id;
        let page_id = id.page_id.unwrap();
        let slot_id = id.slot_id.unwrap();

        let mut container_to_hf_map = self.container_to_hf_map.read().unwrap();

        //first check for valid container
        if !container_to_hf_map.contains_key(&container_id) {
            if !self.id_to_filepath_map.read().unwrap().contains_key(&container_id) {
                panic!("delete_value(): Invalid container ID")
            }
        }

		let mut ref_heap_file = container_to_hf_map.get(&container_id).unwrap();
		let mut heap_file = ref_heap_file.borrow_mut();
        
        let mut hf_ids_to_page_map = heap_file.ids_to_pages_map.read().unwrap();
        let mut temp_page = hf_ids_to_page_map.get(&page_id).unwrap();

        //delete value from page
        temp_page.write().unwrap().delete_value(slot_id);

		Ok(())

    }

    /// Updates a value. Returns valueID on update (which may have changed). Error on failure
    /// Any process that needs to determine if a value changed will need to compare the return valueId against
    /// the sent value.
    fn update_value(
        &self,
        value: Vec<u8>,
        id: ValueId,
        _tid: TransactionId,
    ) -> Result<ValueId, CrustyError> {
        panic!("TODO milestone hs");
    }

    /// Create a new container to be stored.
    /// fn create_container(&self, name: String) -> ContainerId;
    /// Creates a new container object.
    /// For this milestone you will not need to utilize
    /// the container_config, name, container_type, or dependencies
    ///
    ///
    /// # Arguments
    ///
    /// * `container_id` - Id of container to add delta to.
    fn create_container(
        &self,
        container_id: ContainerId,
        _container_config: common::ContainerConfig,
        _name: Option<String>,
        _container_type: common::ids::StateType,
        _dependencies: Option<Vec<ContainerId>>,
    ) -> Result<(), CrustyError> {
        
        if self.container_to_hf_map.read().unwrap().contains_key(&container_id) {
            return Err(CrustyError::ValidationError("SM already contains container with ID {container_id}".to_string()))
        }

        //Create a temp file
        let mut heapfile_path = self.storage_path.clone();
        fs::create_dir_all(self.storage_path.clone());
        heapfile_path.push(gen_rand_string(4));
        heapfile_path.set_extension("hf");

        self.id_to_filepath_map.write().unwrap().insert(container_id, heapfile_path.clone());

        let output_heapfile = HeapFile::new(heapfile_path.to_path_buf(), container_id).expect("Unable to create HF for test");

        self.container_to_hf_map.write().unwrap().insert(container_id, output_heapfile);
        Ok(())
    }

    /// A wrapper function to call create container
    fn create_table(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        self.create_container(
            container_id,
            StorageManager::get_simple_config(),
            None,
            common::ids::StateType::BaseTable,
            None,
        )
    }

    /// Remove the container and all stored values in the container.
    /// If the container is persisted remove the underlying files
    fn remove_container(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        
        let mut container_hf_map = self.container_to_hf_map.write().unwrap();

        let mut file_path_map = self.id_to_filepath_map.write().unwrap();

        //first check for valid container ID's
        if !container_hf_map.contains_key(&container_id) {
            return Err(CrustyError::ValidationError("remove_container: invalid ID".to_string()))
        } else {
            if !file_path_map.contains_key(&container_id) {
                return Err(CrustyError::ValidationError("remove_container: invalid ID".to_string()))
            } else {
                //checks passed, now actually do the deletion
                
                let current_hf = container_hf_map.get(&container_id);

                let file_path_to_delete = file_path_map.get(&container_id).unwrap();

                //delete heapfile
                fs::remove_file(file_path_to_delete);

                container_hf_map.remove(&container_id).unwrap();
                file_path_map.remove(&container_id).unwrap();
            }
        }
        
        Ok(())

    }

    /// Get an iterator that returns all valid records
    fn get_iterator(
        &self,
        container_id: ContainerId,
        tid: TransactionId,
        _perm: Permissions,
    ) -> Self::ValIterator {

        let mut container_to_hf_map = self.container_to_hf_map.write().unwrap();

        //first check for valid container
        if !container_to_hf_map.contains_key(&container_id) {
            if !self.id_to_filepath_map.read().unwrap().contains_key(&container_id) {
                panic!("get_iterator(): Invalid container ID")
            }

            //create new heapfile path and load in the heapfile
            let container_path_map = self.id_to_filepath_map.read().unwrap();
            let filepath = container_path_map.get(&container_id).unwrap();

            let heapfile = HeapFile::new((*filepath).clone(), container_id).unwrap();

            container_to_hf_map.insert(container_id, heapfile);
        }

        //check container id
        if !container_to_hf_map.contains_key(&container_id) {
            panic!("get_iterator(): invalid container_id");
        }
        
        let heapfile = container_to_hf_map.get(&container_id).unwrap();

        let output = HeapFileIterator::new(container_id, tid, Arc::new(heapfile));

        return output;
    }

    /// Get the data for a particular ValueId. Error if does not exists
    fn get_value(
        &self,
        id: ValueId,
        tid: TransactionId,
        perm: Permissions,
    ) -> Result<Vec<u8>, CrustyError> {
        
        //check and access container
        let container_file_map = self.container_to_hf_map.read().unwrap();

        if !container_file_map.contains_key(&id.container_id) {
            return Err(CrustyError::ExecutionError("get_value: invalid container ID".to_string()));
        } else {
            //get page
            let current_hf = container_file_map.get(&id.container_id).unwrap();

            //TODO milestone transaction, use the self.get_page() here later
            let current_page = current_hf.read_page_from_file(id.page_id.unwrap()).unwrap();

            //get slot data in page
            let output = current_page.get_value(id.slot_id.unwrap()).unwrap();

            Ok(output)
        }
    }

    /// Notify the storage manager that the transaction is finished so that any held resources can be released.
    fn transaction_finished(&self, tid: TransactionId) {
        panic!("TODO milestone tm");
    }

    /// Testing utility to reset all state associated the storage manager. Deletes all data in
    /// storage path (keeping storage path as a directory). Doesn't need to serialize any data to
    /// disk as its just meant to clear state. 
    ///
    /// Clear any data structures in the SM you add
    fn reset(&self) -> Result<(), CrustyError> {
		/* 
		let id_paths = self.id_to_filepath_map.read().unwrap(); 
		for id in id_paths.keys() {
			let path = id_paths.get(id).unwrap();
			// Go to parent directory
			let mut path_clone = path.clone();
			path_clone.pop();
			fs::remove_dir_all(path_clone)?;
		}
		drop(id_paths);
		*/

        fs::remove_dir_all(self.storage_path.clone())?;        
		fs::create_dir_all(self.storage_path.clone()).unwrap();

		self.container_to_hf_map.write().unwrap().clear();
		self.id_to_filepath_map.write().unwrap().clear();

		Ok(())
    }

    /// If there is a buffer pool or cache it should be cleared/reset.
    /// Otherwise do nothing.
    fn clear_cache(&self) {
    }

    /// Shutdown the storage manager. Should be safe to call multiple times. You can assume this
    /// function will never be called on a temp SM.
    /// This should serialize the mapping between containerID and Heapfile to disk in a way that
    /// can be read by StorageManager::new. 
    /// HINT: Heapfile won't be serializable/deserializable. You'll want to serialize information
    /// that can be used to create a HeapFile object pointing to the same data. You don't need to
    /// worry about recreating read_count or write_count.
    fn shutdown(&self) {
        
        //need to loop through each heapfile and serialize their information for HeapFile object creation
        let container_to_hf_map = self.container_to_hf_map.read().unwrap();

        for curr_hf_cid in container_to_hf_map.keys() {
            //serialize current hf
            let curr_hf = container_to_hf_map.get(curr_hf_cid).unwrap();

            //loop through all pages in curr_hf and serialize them to their respective files
            let hf_pages_map = curr_hf.ids_to_pages_map.read().unwrap();

			let page_ids_keys = hf_pages_map.keys().into_iter();
			let mut page_ids_only = Vec::from_iter(page_ids_keys);
			page_ids_only.sort();
	
            for curr_page_id in page_ids_only {
                let curr_page = hf_pages_map.get(curr_page_id).unwrap().read().unwrap();

                let curr_page_clone = Page::from_bytes(&curr_page.to_bytes());

                //write page to hf's File object
                curr_hf.write_page_to_file(curr_page_clone);
            }

			//curr_hf.file.write().unwrap().borrow_mut().flush();
		}

        //then need to serialize the sm's metadata, like hashmaps and stuff
        let sm_data= serde_json::to_string(self).unwrap();

        //write the data to the file given by the PathBuf
		let mut sm_meta = self.storage_path.clone();
		let meta_file = Path::new("sm.mta");
		sm_meta.push(meta_file);
        fs::write(sm_meta, sm_data);
    }

    fn import_csv(
        &self,
        table: &Table,
        path: String,
        _tid: TransactionId,
        container_id: ContainerId,
        _timestamp: LogicalTimeStamp,
    ) -> Result<(), CrustyError> {
        // Err(CrustyError::CrustyError(String::from("TODO")))
        // Convert path into an absolute path.
        let path = fs::canonicalize(path)?;
        debug!("server::csv_utils trying to open file, path: {:?}", path);
        let file = fs::File::open(path)?;
        // Create csv reader.
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(file);

        // Iterate through csv records.
        let mut inserted_records = 0;
        for result in rdr.records() {
            #[allow(clippy::single_match)]
            match result {
                Ok(rec) => {
                    // Build tuple and infer types from schema.
                    let mut tuple = Tuple::new(Vec::new());
                    for (field, attr) in rec.iter().zip(table.schema.attributes()) {
                        // TODO: Type mismatch between attributes and record data>
                        match &attr.dtype() {
                            DataType::Int => {
                                let value: i32 = field.parse::<i32>().unwrap();
                                tuple.field_vals.push(Field::IntField(value));
                            }
                            DataType::String => {
                                let value: String = field.to_string().clone();
                                tuple.field_vals.push(Field::StringField(value));
                            }
                        }
                    }
                    //TODO: How should individual row insertion errors be handled?
                    debug!(
                        "server::csv_utils about to insert tuple into container_id: {:?}",
                        &container_id
                    );
                    self.insert_value(container_id, tuple.to_bytes(), _tid);
                    inserted_records += 1;
                }
                _ => {
                    // FIXME: get error from csv reader
                    error!("Could not read row from CSV");
                    return Err(CrustyError::IOError(
                        "Could not read row from CSV".to_string(),
                    ));
                }
            }
        }
        info!("Num records imported: {:?}", inserted_records);
        Ok(())
    }
}

/// Trait Impl for Drop
impl Drop for StorageManager {
    // if temp SM this clears the storage path entirely when it leaves scope; used for testing
    fn drop(&mut self) {
        if self.is_temp {
            debug!("Removing storage path on drop {:?}", self.storage_path);
            let remove_all = fs::remove_dir_all(self.storage_path.clone());
            if let Err(e) = remove_all {
                println!("Error on removing temp dir {}", e);
            }
        }
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use std::io::BufReader;

    use super::*;
    use crate::storage_manager::StorageManager;
    use common::storage_trait::StorageTrait;
    use common::testutil::*;

    #[test]
    fn hs_sm_a_insert() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_table(cid);

        let bytes = get_random_byte_vec(40);
        let tid = TransactionId::new();

        let val1 = sm.insert_value(cid, bytes.clone(), tid);
        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val1.page_id.unwrap());
        assert_eq!(0, val1.slot_id.unwrap());

        let p1 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();

        let val2 = sm.insert_value(cid, bytes, tid);
        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val2.page_id.unwrap());
        assert_eq!(1, val2.slot_id.unwrap());

        let p2 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();
        assert_ne!(p1.to_bytes()[..], p2.to_bytes()[..]);
    }

    #[test]
    fn hs_sm_b_iter_small() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_table(cid);
        let tid = TransactionId::new();

        //Test one page
        let mut byte_vec: Vec<Vec<u8>> = vec![
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
        ];
        for val in &byte_vec {
            sm.insert_value(cid, val.clone(), tid);
        }
        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }

        // Should be on two pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }

        // Should be on 3 pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(300),
            get_random_byte_vec(500),
            get_random_byte_vec(400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }
    }

    #[test]
    #[ignore]
    fn hs_sm_b_iter_large() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;

        sm.create_table(cid).unwrap();
        let tid = TransactionId::new();

        let vals = get_random_vec_of_byte_vec(1000, 40, 400);
        sm.insert_values(cid, vals, tid);
        let mut count = 0;
        for _ in sm.get_iterator(cid, tid, Permissions::ReadOnly) {
            count += 1;
        }
        assert_eq!(1000, count);
    }

    #[derive(Serialize, Deserialize)]
	struct TestEntry {
		len : usize,
		data : Vec<u8>,
	}
	#[derive(Serialize, Deserialize)]
	struct TestData {
		len: usize,
		data: Vec<TestEntry>,
	}

	/*
    #[test]
    fn hs_sm_b_iter_big() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;

        sm.create_table(cid).unwrap();
        let tid = TransactionId::new();

        //let vals = get_random_vec_of_byte_vec(1000, 40, 400);
		let fin = File::open("C:/Users/marbi/Desktop/cmsc23500/test-data.json").expect("Cannot open file");
		let mut reader = BufReader::new(fin);
		//let mut data = String::new();
		//reader.read_to_string(&mut data).unwrap();
		let new_val: TestData = serde_json::from_reader(reader).unwrap();
		
		println!("New length {}", new_val.len);

        // sm.insert_values(cid, vals, tid);
		let mut inserted_count = 0;
		for val in new_val.data {
			let value_id = sm.insert_value(cid, val.data, tid);
			println!("value = {:?} count {}", value_id, inserted_count);
			inserted_count += 1;
		}
        let mut count = 0;
        for _ in sm.get_iterator(cid, tid, Permissions::ReadOnly) {
            count += 1;
			println!("read back count {}", count);
        }
        assert_eq!(1000, count);
    }
	*/
}
