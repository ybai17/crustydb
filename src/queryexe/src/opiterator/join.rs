use super::{OpIterator, TupleIterator};
use common::{CrustyError, Field, SimplePredicateOp, TableSchema, Tuple, table::Table, Attribute, Constraint};
use common::DataType::Int;
use std::cmp::Ordering;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::thread::{JoinHandle, self};
use std::{collections::HashMap, cell::RefCell};
use common::Field::{IntField, StringField};

#[derive(Debug)]
/// Compares the fields of two tuples using a predicate. (You can add any other fields that you think are neccessary)
pub struct JoinPredicate {
    /// Operation to comapre the fields with.
    op: SimplePredicateOp,
    /// Index of the field of the left table (tuple).
    left_index: usize,
    /// Index of the field of the right table (tuple).
    right_index: usize,
}

impl JoinPredicate {
    /// Constructor that determines if two tuples satisfy the join condition.
    ///
    /// # Arguments
    ///
    /// * `op` - Operation to compare the two fields with.
    /// * `left_index` - Index of the field to compare in the left tuple.
    /// * `right_index` - Index of the field to compare in the right tuple.
    fn new(op: SimplePredicateOp, left_index: usize, right_index: usize) -> Self {
        return JoinPredicate {
            op: op,
            left_index: left_index,
            right_index: right_index,
        }
    }
}

/// Nested loop join implementation. (You can add any other fields that you think are neccessary)
pub struct Join {
    /// Join condition.
    predicate: JoinPredicate,
    /// Left child node.
    left_child: Box<dyn OpIterator>,
    /// Right child node.
    right_child: Box<dyn OpIterator>,
    /// Schema of the result.
    schema: TableSchema,
    /// Resulting Vec of the joined tables
    resulting_tuples: Vec<Tuple>,
    /// iterator
    iterator: Option<RefCell<TupleIterator>>,
}

// Helper function to compare values
fn compare(op : SimplePredicateOp, left_key : &Field, right_key : &Field) -> bool {
	match op {
		SimplePredicateOp::All => return true,
		_ => {},
	};

	let cmp_result : Ordering = left_key.cmp(right_key);
	
	match op {
		SimplePredicateOp::Equals => {
			match cmp_result {
				Ordering::Equal => return true,
				_ => return false,
			}
		},
		SimplePredicateOp::NotEq => {
			match cmp_result {
				Ordering::Equal => return false,
				_ => return true,
			}
		},
		SimplePredicateOp::GreaterThan => {
			match cmp_result {
				Ordering::Greater => return true,
				_ => return false,
			}
		},
		SimplePredicateOp::LessThan => {
			match cmp_result {
				Ordering::Less => return true,
				_ => return false,
			}
		},
		SimplePredicateOp::LessThanOrEq => {
			match cmp_result {
				Ordering::Less | Ordering::Equal => return true,
				_ => return false,
			}
		},
		SimplePredicateOp::GreaterThanOrEq => {
			match cmp_result {
				Ordering::Greater | Ordering::Equal => return true,
				_ => return false,
			}
		},
		_ => return true,
	}
}

impl Join {
    /// Join constructor. Creates a new node for a nested-loop join.
    ///
    /// # Arguments
    ///
    /// * `op` - Operation in join condition.
    /// * `left_index` - Index of the left field in join condition.
    /// * `right_index` - Index of the right field in join condition.
    /// * `left_child` - Left child of join operator.
    /// * `right_child` - Left child of join operator.
    pub fn new(
        op: SimplePredicateOp,
        left_index: usize,
        right_index: usize,
        left_child: Box<dyn OpIterator>,
        right_child: Box<dyn OpIterator>,
    ) -> Self {
        
        let mut output_resulting_tuples: Vec<Tuple> = Vec::new();

        let mut left_child_mut = left_child;
        let mut right_child_mut = right_child;
        
        left_child_mut.open();
        right_child_mut.open();

        //stuff for setting the schema data correctly only
        let total_column_counter = left_child_mut.next().unwrap().unwrap().field_vals.len() +
                                          right_child_mut.next().unwrap().unwrap().field_vals.len();

        let mut output_attributes: Vec<Attribute> = Vec::new();

        for i in 0..total_column_counter {
            let new_attr: Attribute = Attribute {
                name: "".to_string(),
                dtype: Int,
                constraint: Constraint::None,
            };
            output_attributes.push(new_attr);
        }

        let mut output_schema = TableSchema::new(output_attributes.clone());

        left_child_mut.rewind();
        right_child_mut.rewind();

        loop {
            let mut left_tuple: Tuple;
            let mut right_tuple: Tuple;

            match left_child_mut.next() {
                Err(e) => panic!("Join.new() left_child.next() error"),
                Ok(o) => {
                    match o {
                        Some(s) => left_tuple = s,
                        None => break,
                    }
                }
            }
			let mut left_key = &left_tuple.field_vals[left_index];
            right_child_mut.rewind();

            loop {
                //need to find all right tuples that fit the join condition
                match right_child_mut.next() {
                    Err(e) => panic!("Join.new() right_child.next() error"),
                    Ok(o) => {
                        match o {
                            Some(s) => right_tuple = s,
                            None => break,
                        }
                    }
                }

				if !compare(op, &left_key, &right_tuple.field_vals[right_index]) {
					continue;
				}

				let mut output_new_fields: Vec<Field> = Vec::new();
				//add left values
				for curr_left_field in left_tuple.field_vals.iter() {
					output_new_fields.push(curr_left_field.clone());
				}

				//add right values
				for curr_right_field in right_tuple.field_vals {
					output_new_fields.push(curr_right_field);
				}

				let output_new_tuple: Tuple = Tuple { field_vals: output_new_fields };

				output_resulting_tuples.push(output_new_tuple);

            } //outside right child loop
        } //outside left child loop

        left_child_mut.close();
        right_child_mut.close();

        let output_predicate = JoinPredicate {
            op: SimplePredicateOp::Equals,
            left_index: left_index,
            right_index: right_index,
        };

        return Join {
            predicate: output_predicate,
            left_child: left_child_mut,
            right_child: right_child_mut,
            schema: output_schema,
            resulting_tuples: output_resulting_tuples,
            iterator: None,
        };
    }
}

impl OpIterator for Join {
    fn open(&mut self) -> Result<(), CrustyError> {
        let mut join_iterator: TupleIterator = TupleIterator::new(self.resulting_tuples.clone(), self.schema.clone());
        join_iterator.open()?;

        self.iterator = Some(RefCell::new(join_iterator));
        Ok(())
    }

    /// Calculates the next tuple for a nested loop join.
    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        match &self.iterator {
            Some(s) => {
                return s.borrow_mut().next();
            },
            None => panic!("Join Iterator: next(), iterator doesn't exist yet")
        }
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        match &self.iterator {
            Some(s) => {
                return s.borrow_mut().close()
            },
            None => panic!("Join Iterator: close(), iterator doesn't exist yet")
        }
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        match &self.iterator {
            Some(s) => {
                return s.borrow_mut().rewind()
            },
            None => panic!("Join Iterator: rewind(), iterator doesn't exist yet")
        }
    }

    /// return schema of the result
    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

/// Hash equi-join implementation. (You can add any other fields that you think are neccessary)
pub struct HashEqJoin {
    predicate: JoinPredicate,

    left_child: Box<dyn OpIterator>,
    right_child: Box<dyn OpIterator>,

    schema: TableSchema,

    resulting_tuples: Vec<Tuple>,
    iterator: Option<RefCell<TupleIterator>>,
}

impl HashEqJoin {
    /// Constructor for a hash equi-join operator.
    ///
    /// # Arguments
    ///
    /// * `op` - Operation in join condition.
    /// * `left_index` - Index of the left field in join condition.
    /// * `right_index` - Index of the right field in join condition.
    /// * `left_child` - Left child of join operator.
    /// * `right_child` - Left child of join operator.
    #[allow(dead_code)]
    pub fn new(
        op: SimplePredicateOp,
        left_index: usize,
        right_index: usize,
        left_child: Box<dyn OpIterator>,
        right_child: Box<dyn OpIterator>,
    ) -> Self {
        let mut output_resulting_tuples: Vec<Tuple> = Vec::new();

        let mut left_child_mut = left_child;
        let mut right_child_mut = right_child;
        
        left_child_mut.open();
        right_child_mut.open();

        //stuff for setting the schema data correctly only
        let total_column_counter = left_child_mut.next().unwrap().unwrap().field_vals.len() +
                                          right_child_mut.next().unwrap().unwrap().field_vals.len();

        let mut output_attributes: Vec<Attribute> = Vec::new();

        for i in 0..total_column_counter {
            let new_attr: Attribute = Attribute {
                name: "".to_string(),
                dtype: Int,
                constraint: Constraint::None,
            };
            output_attributes.push(new_attr);
        }

        let mut output_schema = TableSchema::new(output_attributes.clone());

        left_child_mut.rewind();
        right_child_mut.rewind();

        //each unique leftmost tuple value maps to a vector that stores all the tuples that share the same key
        //these hashmaps will be used later for merging into the resulting table
        let mut right_hashmap: HashMap<Field, RefCell<Vec<Tuple>>> = HashMap::new();

        loop {
            let mut right_tuple: Tuple;

            match right_child_mut.next() {
                Err(e) => panic!("Join.new() right_child.next() error"),
                Ok(o) => {
                    match o {
                        Some(s) => {
                            right_tuple = s;
                            let right_key_field = &right_tuple.field_vals[right_index];
                            if right_hashmap.contains_key(&right_key_field) {
                                // if a tuple with the key is already stored in the hashmap, just add to the vector
                                let mut right_map_vec = right_hashmap.get(&right_key_field).unwrap();
                                right_map_vec.borrow_mut().push(right_tuple);
                            } else {
                                //tuple does not yet exist
                                let mut new_vec: Vec<Tuple> = Vec::new();
                                new_vec.push(right_tuple.clone());
                                right_hashmap.insert(right_key_field.clone(), RefCell::new(new_vec));
                            }
                        },
                        None => break,
                    }
                }
            } //end right child match
        } //end right main loop

        //compare left and right tuples
        //do equals
		loop {
			let mut curr_left_tuple : Tuple;
			match left_child_mut.next() {
				Ok(t) => {
					match t {
						None => break,
						Some(t) => curr_left_tuple = t,
					}
				},
				Err(e) => panic!("Error in executing left_child_mut.next()"),
			}

			let curr_key_field = curr_left_tuple.field_vals[left_index].clone();
            //println!("{:?}", curr_key_field);

            if right_hashmap.contains_key(&curr_key_field) {
                //if right map also contains the key, then join the tuples                
                let mut right_matching_tuples = right_hashmap.get(&curr_key_field).unwrap();

                //now need to merge their data with all the ones in the right tuples
                for curr_right_tuple in right_matching_tuples.borrow_mut().iter() {                    
					let mut output_new_fields: Vec<Field> = Vec::new();
					//add the fields to the output fields to be turned into a tuple
					for curr_left_field in curr_left_tuple.field_vals.iter() {
						output_new_fields.push(curr_left_field.clone());
					}
	
					for curr_right_field in curr_right_tuple.field_vals.iter() {
                        output_new_fields.push(curr_right_field.clone());
                    }
					let resulting_tuple: Tuple = Tuple::new(output_new_fields);
					output_resulting_tuples.push(resulting_tuple);
				}
	        }
		}

        let output_predicate = JoinPredicate {
            op: SimplePredicateOp::Equals,
            left_index: left_index,
            right_index: right_index,
        };

        return HashEqJoin {
            predicate: output_predicate,
            left_child: left_child_mut,
            right_child: right_child_mut,
            schema: output_schema,
            resulting_tuples: output_resulting_tuples,
            iterator: None,
        };
    }
}

impl OpIterator for HashEqJoin {
    fn open(&mut self) -> Result<(), CrustyError> {
        let mut join_iterator: TupleIterator = TupleIterator::new(self.resulting_tuples.clone(), self.schema.clone());
        join_iterator.open()?;

        self.iterator = Some(RefCell::new(join_iterator));
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        match &self.iterator {
            Some(s) => {
                return s.borrow_mut().next();
            },
            None => panic!("HashJoin Iterator: next(), iterator doesn't exist yet")
        }
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        match &self.iterator {
            Some(s) => {
                return s.borrow_mut().close()
            },
            None => panic!("HashJoin Iterator: close(), iterator doesn't exist yet")
        }
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        match &self.iterator {
            Some(s) => {
                //println!("__hashjoin:next()__ {:?}", s.borrow().tuples);
                return s.borrow_mut().rewind()
            },
            None => panic!("HashJoin Iterator: rewind(), iterator doesn't exist yet")
        }
    }

    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

pub struct GraceHashEqJoin {
    predicate: JoinPredicate,

    left_child: Arc<Mutex<Box<dyn OpIterator + Send + Sync>>>,
    right_child: Arc<Mutex<Box<dyn OpIterator + Send + Sync>>>,

    schema: TableSchema,

    left_index: usize,
    right_index: usize,

    resulting_tuples: Option<Vec<Tuple>>,
    iterator: Option<RefCell<TupleIterator>>,
}

impl GraceHashEqJoin {

    /// Constructor for a hash equi-join operator.
    ///
    /// # Arguments
    ///
    /// * `op` - Operation in join condition.
    /// * `left_index` - Index of the left field in join condition.
    /// * `right_index` - Index of the right field in join condition.
    /// * `left_child` - Left child of join operator.
    /// * `right_child` - Left child of join operator.
    #[allow(dead_code)]
    pub fn new(
        op: SimplePredicateOp,
        left_index: usize,
        right_index: usize,
        left_child: Arc<Mutex<Box<dyn OpIterator + Send + Sync>>>,
        right_child: Arc<Mutex<Box<dyn OpIterator + Send + Sync>>>,
    ) -> Self {

        let mut output_resulting_tuples: Vec<Tuple> = Vec::new();
        let mut output_attributes: Vec<Attribute> = Vec::new();

        let mut output_schema;

        {
            let mut left_child_locked = left_child.lock().unwrap();
            let mut right_child_locked = right_child.lock().unwrap();

            left_child_locked.open();
            right_child_locked.open();

            let total_column_counter = left_child_locked.next().unwrap().unwrap().field_vals.len() +
                                              right_child_locked.next().unwrap().unwrap().field_vals.len();

            output_schema = TableSchema::new(output_attributes.clone());

            left_child_locked.rewind();
            right_child_locked.rewind();

            for i in 0..total_column_counter {
                let new_attr = Attribute {
                    name: "".to_string(),
                    dtype: Int,
                    constraint: Constraint::None,
                };
                output_attributes.push(new_attr);
            }
        }

        let output_predicate = JoinPredicate {
            op: SimplePredicateOp::Equals,
            left_index: left_index,
            right_index: right_index,
        };

        return GraceHashEqJoin {
            predicate: output_predicate,
            left_child: left_child,
            right_child: right_child,
            schema: output_schema,
            left_index: left_index,
            right_index: right_index,
            resulting_tuples: None,
            iterator: None,
        };
        
    }

    //called when left child is smaller
    fn run_left_small(&mut self, right_hash_map: HashMap<Field, RefCell<Vec<Tuple>>>) -> Result<Vec<Tuple>, CrustyError> {
        let mut output_resulting_tuples: Vec<Tuple> = Vec::new();

        let mut left_child_locked = self.left_child.lock().unwrap();
        left_child_locked.rewind()?;

        loop {
            let mut left_tuple: Tuple;

            match left_child_locked.next() {
                Ok(v) => match v {
                    Some(t) => left_tuple = t,
                    None => break,
                },
                Err(e) => return Err(e),
            }

            let key = left_tuple.field_vals[self.left_index].clone();

            if !right_hash_map.contains_key(&key) {
                continue;
            }

            let right_tuples = right_hash_map.get(&key).unwrap();
            for mut right_tuple in right_tuples.borrow_mut().iter() {
                let mut fields_joined: Vec<Field> = Vec::new();

                fields_joined.append(&mut left_tuple.field_vals);

                for field in right_tuple. field_vals.iter() {
                    fields_joined.push(field.clone());
                }
                let tuple_joined = Tuple::new(fields_joined);
                output_resulting_tuples.push(tuple_joined);
            }
        }

        return Ok(output_resulting_tuples);
    }

    //called when left child is larger
    fn run_left_large(&mut self, left_hashmap: HashMap<Field, RefCell<Vec<Tuple>>>) -> Result<Vec<Tuple>, CrustyError> {
        let mut output_resulting_tuples: Vec<Tuple> = Vec::new();

        let mut right_child_locked = self.right_child.lock().unwrap();
        right_child_locked.rewind()?;

        loop {
            let mut right_tuple: Tuple;

            match right_child_locked.next() {
                Ok(v) => match v {
                    Some(t) => right_tuple = t,
                    None => break,
                },
                Err(e) => return Err(e),
            }

            let key = right_tuple.field_vals[self.right_index].clone();

            if !left_hashmap.contains_key(&key) {
                continue;
            }

            let left_tuples = left_hashmap.get(&key).unwrap();
            for mut left_tuple in left_tuples.borrow_mut().iter() {

                let mut fields_joined: Vec<Field> = Vec::new();

                for field in left_tuple.field_vals.iter() {
                    fields_joined.push(field.clone());
                }

                fields_joined.append(&mut right_tuple.field_vals);
                let tuple_joined = Tuple::new(fields_joined);
                output_resulting_tuples.push(tuple_joined);
            }
        }

        return Ok(output_resulting_tuples);
    }

    //called since size is not given/known initially
    fn join_unknown_size(&mut self) -> Result<Vec<Tuple>, CrustyError> {
        let left_hashmap;
        let right_hashmap;

        let left_child_clone = self.left_child.clone();
        let thread_left =
            self.create_hash_map_through_thread(left_child_clone, self.left_index);

        let right_child_clone = self.right_child.clone();
        let thread_right =
            self.create_hash_map_through_thread(right_child_clone, self.right_index);

        match thread_left.join().unwrap() {
            Ok(h) => left_hashmap = h,
            Err(e) => return Err(e),
        }

        match thread_right.join().unwrap() {
            Ok(h) => right_hashmap = h,
            Err(e) => return Err(e),
        }

        //now choose which branch based on the size differences, if any
        if left_hashmap.len() < right_hashmap.len() {
            return self.run_left_small(right_hashmap);
        } else {
            return self.run_left_large(left_hashmap);
        }
    }

    //creates a thread that iterates through the the child; then creates a hashmap
    //left and right will both call
    fn create_hash_map_through_thread(&self, child: Arc<Mutex<Box<dyn OpIterator + Send + Sync>>>, key_index: usize) ->
        JoinHandle<Result<HashMap<Field, RefCell<Vec<Tuple>>>, CrustyError>>
    {
        let thread_hashmap = thread::spawn(move || {
            let mut child_locked = child.lock().unwrap();
            child_locked.rewind()?;

            let mut map: HashMap<Field, RefCell<Vec<Tuple>>> = HashMap::new();

            loop {
                match child_locked.next() {
                    Ok(v) => match v {
                        Some(t) => {
                            let key = t.field_vals[key_index].clone();
                            if !map.contains_key(&key) {
                                map.insert(key, RefCell::new(vec![t]));
                            } else {
                                let mut vec_tuple = map.get(&key).unwrap().borrow_mut();
                                vec_tuple.push(t);
                            }
                        },
                        None => break,
                    },
                    Err(e) => return Err(e),
                }
            }
            return Ok(map);
        }); //end thread_hashmap
        return thread_hashmap;
    }
}

impl OpIterator for GraceHashEqJoin {
    fn open(&mut self) -> Result<(), CrustyError> {
        let output_resulting_tuples: Vec<Tuple>;

        match self.join_unknown_size() {
            Ok(t) => output_resulting_tuples = t,
            Err(e) => return Err(e),
        }

        let mut join_iterator: TupleIterator = TupleIterator::new(output_resulting_tuples, self.schema.clone());
        join_iterator.open()?;

        self.iterator = Some(RefCell::new(join_iterator));
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        match &self.iterator {
            Some(s) => return s.borrow_mut().next(),
            None => panic!("GraceHashEqJoin next(): Iterator doesn't exist yet"),
        }
    }

    fn close(&mut self) -> Result<(), CrustyError> {

        match &self.iterator {
            Some(s) => return s.borrow_mut().close(),
            None => panic!("GraceHashEqJoin close: Iterator doesn't exist yet"),
        }
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {

        match &self.iterator {
            Some(s) => return s.borrow_mut().rewind(),
            None => panic!("GraceHashEqJoin rewind: Iterator doesn't exist yet"),
        }
    }

    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::opiterator::testutil::*;
    use common::testutil::*;

    const WIDTH1: usize = 2;
    const WIDTH2: usize = 3;
    enum JoinType {
        NestedLoop,
        HashEq,
    }

    pub fn scan1() -> TupleIterator {
        let tuples = create_tuple_list(vec![vec![1, 2], vec![3, 4], vec![5, 6], vec![7, 8]]);
        let ts = get_int_table_schema(WIDTH1);
        TupleIterator::new(tuples, ts)
    }

    pub fn scan2() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![1, 2, 3],
            vec![2, 3, 4],
            vec![3, 4, 5],
            vec![4, 5, 6],
            vec![5, 6, 7],
        ]);
        let ts = get_int_table_schema(WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    pub fn eq_join() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![1, 2, 1, 2, 3],
            vec![3, 4, 3, 4, 5],
            vec![5, 6, 5, 6, 7],
        ]);
        let ts = get_int_table_schema(WIDTH1 + WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    pub fn gt_join() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![3, 4, 1, 2, 3], // 1, 2 < 3
            vec![3, 4, 2, 3, 4],
            vec![5, 6, 1, 2, 3], // 1, 2, 3, 4 < 5
            vec![5, 6, 2, 3, 4],
            vec![5, 6, 3, 4, 5],
            vec![5, 6, 4, 5, 6],
            vec![7, 8, 1, 2, 3], // 1, 2, 3, 4, 5 < 7
            vec![7, 8, 2, 3, 4],
            vec![7, 8, 3, 4, 5],
            vec![7, 8, 4, 5, 6],
            vec![7, 8, 5, 6, 7],
        ]);
        let ts = get_int_table_schema(WIDTH1 + WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    pub fn lt_join() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![1, 2, 2, 3, 4], // 1 < 2, 3, 4, 5
            vec![1, 2, 3, 4, 5],
            vec![1, 2, 4, 5, 6],
            vec![1, 2, 5, 6, 7],
            vec![3, 4, 4, 5, 6], // 3 < 4, 5
            vec![3, 4, 5, 6, 7],
        ]);
        let ts = get_int_table_schema(WIDTH1 + WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    pub fn lt_or_eq_join() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![1, 2, 1, 2, 3], // 1 <= 1, 2, 3, 4, 5
            vec![1, 2, 2, 3, 4],
            vec![1, 2, 3, 4, 5],
            vec![1, 2, 4, 5, 6],
            vec![1, 2, 5, 6, 7],
            vec![3, 4, 3, 4, 5], // 3 <= 3, 4, 5
            vec![3, 4, 4, 5, 6],
            vec![3, 4, 5, 6, 7],
            vec![5, 6, 5, 6, 7], // 5 <= 5
        ]);
        let ts = get_int_table_schema(WIDTH1 + WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    fn construct_join(
        ty: JoinType,
        op: SimplePredicateOp,
        left_index: usize,
        right_index: usize,
    ) -> Box<dyn OpIterator> {
        let s1 = Box::new(scan1());
        let s2 = Box::new(scan2());
        match ty {
            JoinType::NestedLoop => Box::new(Join::new(op, left_index, right_index, s1, s2)),
            JoinType::HashEq => Box::new(HashEqJoin::new(op, left_index, right_index, s1, s2)),
        }
    }

    fn test_get_schema(join_type: JoinType) {
        let op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        let expected = get_int_table_schema(WIDTH1 + WIDTH2);
        let actual = op.get_schema();
        assert_eq!(&expected, actual);
    }

    fn test_next_not_open(join_type: JoinType) {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        op.next().unwrap();
    }

    fn test_close_not_open(join_type: JoinType) {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        op.close().unwrap();
    }

    fn test_rewind_not_open(join_type: JoinType) {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        op.rewind().unwrap();
    }

    fn test_rewind(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        op.open()?;
        while op.next()?.is_some() {}
        op.rewind()?;

        let mut eq_join = eq_join();
        eq_join.open()?;

        let acutal = op.next()?;
        let expected = eq_join.next()?;
        
        assert_eq!(acutal, expected);
        Ok(())
    }

    fn test_eq_join(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        let mut eq_join = eq_join();
        op.open()?;
        eq_join.open()?;
        match_all_tuples(op, Box::new(eq_join))
    }

    fn test_gt_join(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::GreaterThan, 0, 0);
        let mut gt_join = gt_join();
        op.open()?;
        gt_join.open()?;
        match_all_tuples(op, Box::new(gt_join))
    }

    fn test_lt_join(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::LessThan, 0, 0);
        let mut lt_join = lt_join();
        op.open()?;
        lt_join.open()?;
        match_all_tuples(op, Box::new(lt_join))
    }

    fn test_lt_or_eq_join(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::LessThanOrEq, 0, 0);
        let mut lt_or_eq_join = lt_or_eq_join();
        op.open()?;
        lt_or_eq_join.open()?;
        match_all_tuples(op, Box::new(lt_or_eq_join))
    }

    mod join {
        use super::*;

        #[test]
        fn get_schema() {
            test_get_schema(JoinType::NestedLoop);
        }

        #[test]
        #[should_panic]
        fn next_not_open() {
            test_next_not_open(JoinType::NestedLoop);
        }

        #[test]
        #[should_panic]
        fn close_not_open() {
            test_close_not_open(JoinType::NestedLoop);
        }

        #[test]
        #[should_panic]
        fn rewind_not_open() {
            test_rewind_not_open(JoinType::NestedLoop);
        }

        #[test]
        fn rewind() -> Result<(), CrustyError> {
            test_rewind(JoinType::NestedLoop)
        }

        #[test]
        fn eq_join() -> Result<(), CrustyError> {
            test_eq_join(JoinType::NestedLoop)
        }

        #[test]
        fn gt_join() -> Result<(), CrustyError> {
            test_gt_join(JoinType::NestedLoop)
        }

        #[test]
        fn lt_join() -> Result<(), CrustyError> {
            test_lt_join(JoinType::NestedLoop)
        }

        #[test]
        fn lt_or_eq_join() -> Result<(), CrustyError> {
            test_lt_or_eq_join(JoinType::NestedLoop)
        }
    }

    mod hash_join {
        use super::*;

        #[test]
        fn get_schema() {
            test_get_schema(JoinType::HashEq);
        }

        #[test]
        #[should_panic]
        fn next_not_open() {
            test_next_not_open(JoinType::HashEq);
        }

        #[test]
        #[should_panic]
        fn rewind_not_open() {
            test_rewind_not_open(JoinType::HashEq);
        }

        #[test]
        fn rewind() -> Result<(), CrustyError> {
            test_rewind(JoinType::HashEq)
        }

        #[test]
        fn eq_join() -> Result<(), CrustyError> {
            test_eq_join(JoinType::HashEq)
        }
    }

    mod grace_hash_join {
        use super::*;
        #[test]
        fn eq_join() -> Result<(), CrustyError> {
            let left_child: Arc<Mutex<Box<dyn OpIterator + Send + Sync>>> = Arc::new(Mutex::new(Box::new(scan1())));
            let right_child: Arc<Mutex<Box<dyn OpIterator + Send + Sync>>> = Arc::new(Mutex::new(Box::new(scan2())));

            let mut grace = GraceHashEqJoin::new(SimplePredicateOp::Equals, 0, 0, left_child, right_child);
            grace.open()?;

            let mut eq_join = test::eq_join();
            eq_join.open()?;

            return match_all_tuples(Box::new(grace), Box::new(eq_join));
        }
    }
}
