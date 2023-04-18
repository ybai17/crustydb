use super::{OpIterator, TupleIterator};
use common::table::Table;
use common::{AggOp, Attribute, CrustyError, DataType, Field, TableSchema, Tuple};
use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::cmp::{max, min, Ordering};
use std::collections::HashMap;
use std::sync::{RwLock, Arc};
use common::Field::{StringField, IntField};

//Wrapper class for storing multiple AggValues for use in aggregation over a single field 
pub struct AggValues {
    data: Vec<AggValue>,
}

//serves as a general-purpose struct for storing any of the values in a field to be aggregated in some form
#[derive(Clone, Debug)]
pub struct AggValue {
    max: i32,
    min: i32,
    avg: i32,
    sum: i32,
    count: i32,
    string_min: RefCell<Option<String>>,
    string_max: RefCell<Option<String>>,
    is_num: bool,
}

/// Contains the index of the field to aggregate and the operator to apply to the column of each group. (You can add any other fields that you think are neccessary)
#[derive(Clone, Debug)]
pub struct AggregateField {
    /// Index of field being aggregated.
    pub field: usize,
    /// Agregate operation to aggregate the column with.
    pub op: AggOp,
}

/// Computes an aggregation function over multiple columns and grouped by multiple fields. (You can add any other fields that you think are neccessary)
#[derive(Debug)]
struct Aggregator {
    /// Aggregated fields.
    agg_fields: Vec<AggregateField>,
    /// Group by fields
    groupby_fields: Vec<usize>,
    /// Schema of the output.
    schema: TableSchema,
    /// Vector of tuples 
    agg_data: Vec<Tuple>,
}

impl AggValue {
    pub fn assign_string_min(&mut self, assigned_str: &String) {
        self.string_min = RefCell::new(Some(assigned_str.clone()));
    }

    pub fn assign_string_max(&mut self, assigned_str: &String) {
        self.string_max = RefCell::new(Some(assigned_str.clone()));
    }

    pub fn get_string_min(&self) -> Option<String> {
        return Some((self.string_min.borrow().as_ref().unwrap()).clone());
    }

    pub fn get_string_max(&self) -> Option<String> {
        return Some((self.string_max.borrow().as_ref().unwrap()).clone());
    }
    
    pub fn compare(&mut self, string: &String) {

        let mut assign: bool = false;
        
        //first compare min
        {
            let string_to_compare_ref = self.string_min.borrow();
            let string_to_compare_result = string_to_compare_ref.as_ref().unwrap().cmp(string); 

            match string_to_compare_result {
                Ordering::Less => {},
                Ordering::Greater => {
                    assign = true;
                },
                Ordering::Equal => {},
            }
        }
        if assign {
            self.string_min = RefCell::new(Some(string.to_string()));
        }
        
        //then compare max
        {
            let string_to_compare_ref = self.string_max.borrow();
            let string_to_compare_result = string_to_compare_ref.as_ref().unwrap().cmp(string);

            match string_to_compare_result {
                Ordering::Less => {
                    assign = true;
                },
                Ordering::Greater => {},
                Ordering::Equal => {},
            }
        }
        if assign {
            self.string_max = RefCell::new(Some(string.to_string()));
        }
        
    }
}

impl Aggregator {
    /// Aggregator constructor.
    ///
    /// # Arguments
    ///
    /// * `agg_fields` - List of `AggregateField`s to aggregate over. `AggregateField`s contains the aggregation function and the field to aggregate over.
    /// * `groupby_fields` - Indices of the fields to groupby over.
    /// * `schema` - TableSchema of the form [groupby_field attributes ..., agg_field attributes ...]).
    fn new(
        agg_fields: Vec<AggregateField>,
        groupby_fields: Vec<usize>,
        schema: &TableSchema,
    ) -> Self {

        let output_data: Vec<Tuple> = Vec::new();
        
        let output = Aggregator {
            agg_fields: agg_fields,
            groupby_fields: groupby_fields,
            schema: schema.clone(),
            agg_data: output_data,
        };

        return output;

    }


    /// Handles the creation of groups for aggregation.
    ///
    /// If a group exists, then merge the tuple into the group's accumulated value.
    /// Otherwise, create a new group aggregate result.
    ///
    /// # Arguments
    ///
    /// * `tuple` - Tuple to add to a group.
    pub fn merge_tuple_into_group(&mut self, tuple: &Tuple) {
        self.agg_data.push(tuple.clone());
    }

    /// Returns a `TupleIterator` over the results.
    ///
    /// Resulting tuples must be of the form: (group by fields ..., aggregate fields ...)
    pub fn iterator(&self) -> TupleIterator {
        
        let mut groupby_field_to_agg_field_map: HashMap<Vec<Field>, RwLock<AggValues>> = HashMap::new();

        for curr_tuple in self.agg_data.iter() {
            let mut key: Vec<Field> = Vec::new();

            for curr_field in self.groupby_fields.iter() {
                key.push(curr_tuple.field_vals[*curr_field].clone());
            }

            //first row
            if !groupby_field_to_agg_field_map.contains_key(&key) {
                let mut tuple_agg_values_data: Vec<AggValue> = Vec::new();

                let mut tuple_agg_values = AggValues {
                    data: tuple_agg_values_data,
                };
                
                //perform aggregate function on this tuple's to-be-aggregated fields
                for curr_agg_field in self.agg_fields.iter() {
                    let value = curr_tuple.field_vals[curr_agg_field.field].clone();

                    let mut unwrapped_value_int: i32 = 0;
                    let mut unwrapped_value_str: String = "".to_string();

                    let mut agg_val_is_num: bool = false;

                    match value {
                        IntField(v) => {
                            unwrapped_value_int = value.unwrap_int_field();
                            agg_val_is_num = true;
                        },
                        StringField(s) => unwrapped_value_str = s.clone(),
                    }

                    //initial agg value
                    let mut agg_val: AggValue = AggValue {
                        max: unwrapped_value_int,
                        min: unwrapped_value_int,
                        avg: unwrapped_value_int,
                        sum: unwrapped_value_int,
                        count: 1,
                        string_min: RefCell::new(None),
                        string_max: RefCell::new(None),
                        is_num: agg_val_is_num,
                    };

                    if !agg_val.is_num {
                        agg_val.assign_string_min(&unwrapped_value_str);
                        agg_val.assign_string_max(&unwrapped_value_str);
                    }

                    tuple_agg_values.data.push(agg_val);
                }
                
                groupby_field_to_agg_field_map.insert(key, RwLock::new(tuple_agg_values));
            } else {
                //hashmap already contains this pair of a field to be aggregated and the actual aggregate values
                let mut tuple_agg_values_accessor = groupby_field_to_agg_field_map.get(&key).unwrap();

                let mut tuple_agg_value_index = 0;

                for curr_agg_field in self.agg_fields.iter() {

                    let value = curr_tuple.field_vals[curr_agg_field.field].clone();

                    let mut tuple_agg_values_locked = tuple_agg_values_accessor.write().unwrap();

                    tuple_agg_values_locked.data[tuple_agg_value_index].count += 1;

                    //check if we're aggregating a number value or string value
                    //num val
                    if tuple_agg_values_locked.data[tuple_agg_value_index].is_num {

                        let value_unwrapped_int = value.unwrap_int_field();

                        if value_unwrapped_int < tuple_agg_values_locked.data[tuple_agg_value_index].min {
                            tuple_agg_values_locked.data[tuple_agg_value_index].min = value_unwrapped_int;
                        }

                        if value_unwrapped_int > tuple_agg_values_locked.data[tuple_agg_value_index].max {
                            tuple_agg_values_locked.data[tuple_agg_value_index].max = value_unwrapped_int;
                        }
                        
                        tuple_agg_values_locked.data[tuple_agg_value_index].sum += value_unwrapped_int;
                    } else {  
                        //string value
                        let value_unwrapped_str: String = value.unwrap_string_field().to_string();

                        //compare and track the min and max strings
                        tuple_agg_values_locked.data[tuple_agg_value_index].compare(&value_unwrapped_str);
                    }

                    tuple_agg_value_index += 1;
                    //avg later when generating the output
                } //outside 

            }
        }

        let mut output_tuples: Vec<Tuple> = Vec::new();

        for (key, value) in groupby_field_to_agg_field_map.iter() {

            let mut tuple_to_push_data: Vec<Field> = Vec::new();

            for curr_key_field in key {
                tuple_to_push_data.push(curr_key_field.clone());

            }

            let mut agg_value_index = 0;

            //handle average here
            for curr_value_field in self.agg_fields.iter() {
                let mut agg_value_unwrapped: i32 = 0;

                let value_data_unlocked = value.read().unwrap();

                let output_field : Field;
                match curr_value_field.op {
                    AggOp::Avg => {
                        output_field = IntField(value_data_unlocked.data[agg_value_index].sum / value_data_unlocked.data[agg_value_index].count);
                    },
                    AggOp::Count => {
                        output_field = IntField(value_data_unlocked.data[agg_value_index].count);
                    },
                    AggOp::Sum => {
                        output_field = IntField(value_data_unlocked.data[agg_value_index].sum);
                    },
                    AggOp::Max => {
                        if value_data_unlocked.data[agg_value_index].is_num {
                            output_field = IntField(value_data_unlocked.data[agg_value_index].max);
                        }
                        else {
                            output_field = StringField(value_data_unlocked.data[agg_value_index].get_string_max().unwrap());
                        }
                    },
                    AggOp::Min => {
                        if value_data_unlocked.data[agg_value_index].is_num {
                            output_field = IntField(value_data_unlocked.data[agg_value_index].min);
                        } else {
                            output_field = StringField(value_data_unlocked.data[agg_value_index].get_string_min().unwrap());
                        }
                    },
                }

                tuple_to_push_data.push(output_field);

                agg_value_index += 1;
            }

            let tuple_to_push = Tuple {
                field_vals: tuple_to_push_data,
            };

            output_tuples.push(tuple_to_push);
        }
        
        let output = TupleIterator::new(output_tuples, self.schema.clone());

        return output;

    }
}

/// Aggregate operator. (You can add any other fields that you think are neccessary)
pub struct Aggregate {
    /// Fields to groupby over.
    groupby_fields: Vec<usize>,
    /// Aggregation fields and corresponding aggregation functions.
    agg_fields: Vec<AggregateField>,
    /// Aggregation iterators for results.
    agg_iter: Option<TupleIterator>,
    /// Output schema of the form [groupby_field attributes ..., agg_field attributes ...]).
    schema: TableSchema,
    /// Boolean if the iterator is open.
    open: bool,
    /// Child operator to get the data from.
    child: Box<dyn OpIterator>,
    ///Aggregator
    agg: RefCell<Aggregator>,
}

impl Aggregate {
    /// Aggregate constructor.
    ///
    /// # Arguments
    ///
    /// * `groupby_indices` - the indices of the group by fields
    /// * `groupby_names` - the names of the group_by fields in the final aggregation
    /// * `agg_indices` - the indices of the aggregate fields
    /// * `agg_names` - the names of the aggreagte fields in the final aggregation
    /// * `ops` - Aggregate operations, 1:1 correspondence with the indices in agg_indices
    /// * `child` - child operator to get the input data from.
    pub fn new(
        groupby_indices: Vec<usize>,
        groupby_names: Vec<&str>,
        agg_indices: Vec<usize>,
        agg_names: Vec<&str>,
        ops: Vec<AggOp>,
        child: Box<dyn OpIterator>,
    ) -> Self {
        let mut aggregator_agg_fields: Vec<AggregateField> = Vec::new();

        for agg_field_index in 0..agg_indices.len() {
            let curr_aggregatefield: AggregateField = AggregateField {
                field: agg_indices[agg_field_index],
                op: ops[agg_field_index],
            };

            aggregator_agg_fields.push(curr_aggregatefield);
        }

        let aggregator_table_schema: TableSchema = TableSchema::new(Vec::new());

        let aggregator = Aggregator::new(aggregator_agg_fields.clone(), groupby_indices.clone(), &aggregator_table_schema);

        let output: Aggregate = Aggregate {
            groupby_fields: groupby_indices.clone(),
            agg_fields: aggregator_agg_fields,
            agg_iter: None,
            schema: aggregator_table_schema,
            open: false,
            child: child,
            agg: RefCell::new(aggregator),
        };

        return output;
    }

}

impl OpIterator for Aggregate {
    fn open(&mut self) -> Result<(), CrustyError> {
        
        if !self.open {
            self.open = true;

            let mut aggregator = &self.agg;
            
            self.child.open();

            loop {
                match self.child.next() {
                    Ok(o) => {
                        match o {
                            None => break,
                            Some(curr_tuple) => {
                                aggregator.borrow_mut().merge_tuple_into_group(&curr_tuple);
                            },
                        }
                    },
                    Err(e) => panic!("Aggregate.open() error with child iterator"),
                }
            }

            let mut agg_iterator = aggregator.borrow().iterator();

            agg_iterator.open();
            self.agg_iter = Some(agg_iterator);
        }

        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if self.open {
            let agg_iter = self.agg_iter.as_mut();

            let output_tuple = agg_iter.unwrap().next();
            return output_tuple
        } else {
            panic!("Aggregate.next(): iterator is closed");
        }
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        if self.open {
            self.open = false;

            return self.agg_iter.as_mut().unwrap().close();
        } else {
            panic!("Aggregate.close(): iterator already closed");
        }
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        
        if self.open {
            return self.agg_iter.as_mut().unwrap().rewind();
        } else {
            panic!("Aggregate.rewind(): Iterator is not open yet");
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

    /// Creates a vector of tuples to create the following table:
    ///
    /// 1 1 3 E
    /// 2 1 3 G
    /// 3 1 4 A
    /// 4 2 4 G
    /// 5 2 5 G
    /// 6 2 5 G
    fn tuples() -> Vec<Tuple> {
        let tuples = vec![
            Tuple::new(vec![
                Field::IntField(1),
                Field::IntField(1),
                Field::IntField(3),
                Field::StringField("E".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(2),
                Field::IntField(1),
                Field::IntField(3),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(3),
                Field::IntField(1),
                Field::IntField(4),
                Field::StringField("A".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(4),
                Field::IntField(2),
                Field::IntField(4),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(5),
                Field::IntField(2),
                Field::IntField(5),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(6),
                Field::IntField(2),
                Field::IntField(5),
                Field::StringField("G".to_string()),
            ]),
        ];
        tuples
    }

    mod aggregator {
        use super::*;
        use common::{DataType, Field};

        /// Set up testing aggregations without grouping.
        ///
        /// # Arguments
        ///
        /// * `op` - Aggregation Operation.
        /// * `field` - Field do aggregation operation over.
        /// * `expected` - The expected result.
        fn test_no_group(op: AggOp, field: usize, expected: i32) -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![Attribute::new("agg".to_string(), DataType::Int)]);
            let mut agg = Aggregator::new(vec![AggregateField { field, op }], Vec::new(), &schema);
            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            assert_eq!(
                Field::IntField(expected),
                *ai.next()?.unwrap().get_field(0).unwrap()
            );
            assert_eq!(None, ai.next()?);
            Ok(())
        }

        #[test]
        fn test_merge_tuples_count() -> Result<(), CrustyError> {
            test_no_group(AggOp::Count, 0, 6)
        }

        #[test]
        fn test_merge_tuples_sum() -> Result<(), CrustyError> {
            test_no_group(AggOp::Sum, 1, 9)
        }

        #[test]
        fn test_merge_tuples_max() -> Result<(), CrustyError> {
            test_no_group(AggOp::Max, 0, 6)
        }

        #[test]
        fn test_merge_tuples_min() -> Result<(), CrustyError> {
            test_no_group(AggOp::Min, 0, 1)
        }

        #[test]
        fn test_merge_tuples_avg() -> Result<(), CrustyError> {
            test_no_group(AggOp::Avg, 0, 3)
        }

        #[test]
        #[should_panic]
        fn test_merge_tuples_not_int() {
            let _ = test_no_group(AggOp::Avg, 3, 3);
        }

        #[test]
        fn test_merge_multiple_ops() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("agg1".to_string(), DataType::Int),
                Attribute::new("agg2".to_string(), DataType::Int),
            ]);

            let mut agg = Aggregator::new(
                vec![
                    AggregateField {
                        field: 0,
                        op: AggOp::Max,
                    },
                    AggregateField {
                        field: 3,
                        op: AggOp::Count,
                    },
                ],
                Vec::new(),
                &schema,
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let expected = vec![Field::IntField(6), Field::IntField(6)];
            let mut ai = agg.iterator();
            ai.open()?;
            assert_eq!(Tuple::new(expected), ai.next()?.unwrap());
            Ok(())
        }

        #[test]
        fn test_merge_tuples_one_group() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("group".to_string(), DataType::Int),
                Attribute::new("agg".to_string(), DataType::Int),
            ]);
            let mut agg = Aggregator::new(
                vec![AggregateField {
                    field: 0,
                    op: AggOp::Sum,
                }],
                vec![2],
                &schema,
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            let rows = num_tuples(&mut ai)?;
            assert_eq!(3, rows);
            Ok(())
        }

        /// Returns the count of the number of tuples in an OpIterator.
        ///
        /// This function consumes the iterator.
        ///
        /// # Arguments
        ///
        /// * `iter` - Iterator to count.
        pub fn num_tuples(iter: &mut impl OpIterator) -> Result<u32, CrustyError> {
            let mut counter = 0;
            while iter.next()?.is_some() {
                counter += 1;
            }
            Ok(counter)
        }

        #[test]
        fn test_merge_tuples_multiple_groups() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("group1".to_string(), DataType::Int),
                Attribute::new("group2".to_string(), DataType::Int),
                Attribute::new("agg".to_string(), DataType::Int),
            ]);

            let mut agg = Aggregator::new(
                vec![AggregateField {
                    field: 0,
                    op: AggOp::Sum,
                }],
                vec![1, 2],
                &schema,
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            let rows = num_tuples(&mut ai)?;
            assert_eq!(4, rows);
            Ok(())
        }
    }

    mod aggregate {
        use super::super::TupleIterator;
        use super::*;
        use common::{DataType, Field};

        fn tuple_iterator() -> TupleIterator {
            let names = vec!["1", "2", "3", "4"];
            let dtypes = vec![
                DataType::Int,
                DataType::Int,
                DataType::Int,
                DataType::String,
            ];
            let schema = TableSchema::from_vecs(names, dtypes);
            let tuples = tuples();
            TupleIterator::new(tuples, schema)
        }

        #[test]
        fn test_open() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            assert!(!ai.open);
            ai.open()?;
            assert!(ai.open);
            Ok(())
        }

        fn test_single_agg_no_group(
            op: AggOp,
            op_name: &str,
            col: usize,
            expected: Field,
        ) -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![col],
                vec![op_name],
                vec![op],
                Box::new(ti),
            );
            ai.open()?;
            assert_eq!(
                // Field::IntField(expected),
                expected,
                *ai.next()?.unwrap().get_field(0).unwrap()
            );
            assert_eq!(None, ai.next()?);
            Ok(())
        }

        #[test]
        fn test_single_agg() -> Result<(), CrustyError> {
            test_single_agg_no_group(AggOp::Count, "count", 0, Field::IntField(6))?;
            test_single_agg_no_group(AggOp::Sum, "sum", 0, Field::IntField(21))?;
            test_single_agg_no_group(AggOp::Max, "max", 0, Field::IntField(6))?;
            test_single_agg_no_group(AggOp::Min, "min", 0, Field::IntField(1))?;
            test_single_agg_no_group(AggOp::Avg, "avg", 0, Field::IntField(3))?;
            test_single_agg_no_group(AggOp::Count, "count", 3, Field::IntField(6))?;
            test_single_agg_no_group(AggOp::Max, "max", 3, Field::StringField("G".to_string()))?;
            test_single_agg_no_group(AggOp::Min, "min", 3, Field::StringField("A".to_string()))
        }

        #[test]
        fn test_multiple_aggs() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![3, 0, 0],
                vec!["count", "avg", "max"],
                vec![AggOp::Count, AggOp::Avg, AggOp::Max],
                Box::new(ti),
            );
            ai.open()?;
            let first_row: Vec<Field> = ai.next()?.unwrap().field_vals().cloned().collect();
            assert_eq!(
                vec![Field::IntField(6), Field::IntField(3), Field::IntField(6)],
                first_row
            );
            ai.close()
        }

        /// Consumes an OpIterator and returns a corresponding 2D Vec of fields
        pub fn iter_to_vec(iter: &mut impl OpIterator) -> Result<Vec<Vec<Field>>, CrustyError> {
            let mut rows = Vec::new();
            iter.open()?;
            while let Some(t) = iter.next()? {
                rows.push(t.field_vals().cloned().collect());
            }
            iter.close()?;
            Ok(rows)
        }

        #[test]
        fn test_multiple_aggs_groups() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                vec![1, 2],
                vec!["group1", "group2"],
                vec![3, 0],
                vec!["count", "max"],
                vec![AggOp::Count, AggOp::Max],
                Box::new(ti),
            );
            let mut result = iter_to_vec(&mut ai)?;
            result.sort();
            let expected = vec![
                vec![
                    Field::IntField(1),
                    Field::IntField(3),
                    Field::IntField(2),
                    Field::IntField(2),
                ],
                vec![
                    Field::IntField(1),
                    Field::IntField(4),
                    Field::IntField(1),
                    Field::IntField(3),
                ],
                vec![
                    Field::IntField(2),
                    Field::IntField(4),
                    Field::IntField(1),
                    Field::IntField(4),
                ],
                vec![
                    Field::IntField(2),
                    Field::IntField(5),
                    Field::IntField(2),
                    Field::IntField(6),
                ],
            ];
            assert_eq!(expected, result);
            ai.open()?;
            let num_rows = num_tuples(&mut ai)?;
            ai.close()?;
            assert_eq!(4, num_rows);
            Ok(())
        }

        #[test]
        #[should_panic]
        fn test_next_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.next().unwrap();
        }

        #[test]
        fn test_close() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.open()?;
            assert!(ai.open);
            ai.close()?;
            assert!(!ai.open);
            Ok(())
        }

        #[test]
        #[should_panic]
        fn test_close_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.close().unwrap();
        }

        #[test]
        #[should_panic]
        fn test_rewind_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.rewind().unwrap();
        }

        #[test]
        fn test_rewind() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                vec![2],
                vec!["group"],
                vec![3],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.open()?;
            let count_before = num_tuples(&mut ai);
            ai.rewind()?;
            let count_after = num_tuples(&mut ai);
            ai.close()?;
            assert_eq!(count_before, count_after);
            Ok(())
        }

        #[test]
        fn test_get_schema() {
            let mut agg_names = vec!["count", "max"];
            let mut groupby_names = vec!["group1", "group2"];
            let ti = tuple_iterator();
            let ai = Aggregate::new(
                vec![1, 2],
                groupby_names.clone(),
                vec![3, 0],
                agg_names.clone(),
                vec![AggOp::Count, AggOp::Max],
                Box::new(ti),
            );
            groupby_names.append(&mut agg_names);
            let expected_names = groupby_names;
            let schema = ai.get_schema();
            for (i, attr) in schema.attributes().enumerate() {
                assert_eq!(expected_names[i], attr.name());
                assert_eq!(DataType::Int, *attr.dtype());
            }
        }
    }
}
