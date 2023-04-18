use std::sync::{Arc, Mutex};

use criterion::{black_box, Criterion};
use rand::Rng;

use common::{Tuple, Field, TableSchema, Attribute, DataType};

use common::testutil::gen_uniform_ints;
use queryexe::opiterator::{OpIterator, TupleIterator, GraceHashEqJoin};

//set up some conditions for tables to iterate through
fn get_random_tuple_iterators() -> Vec<Vec<Tuple>> {
	// a selection of numbers of tuples to generate to choose from

	let tuple_count : Vec<u32> = vec![5, 30, 200, 2000, 9000];
	let table_count : Vec<u32> = vec![2, 2, 2, 2, 2];

	let mut rng = rand::thread_rng();
	let mut candiate_tuples : Vec<Vec<Tuple>> = Vec::new();

	for index in 0..tuple_count.len() {

		for num in 0..table_count[index] {
			let size = tuple_count[index];
			let key_upper_bound = 2*size;

			// Generate so that some keys will not meet
			let mut original = gen_uniform_ints(key_upper_bound as u64, Some(key_upper_bound as u64));
			
			// Remove extra number randomly
			let mut total = key_upper_bound;
			let mut removed : u32 = 0;

			// loop through and remove half of the keys randomly
			// to make sure some keys match and some don't
			for removed in 0..size {
				let rn = rng.gen_range(0..total) as usize;
				original.remove(rn);
				total -= 1;
			}

			// Create Tuple which have the same key and values
			let mut one_candidate : Vec<Tuple> = Vec::new();
			for f in original.iter() {
				let mut v : Vec<Field> = Vec::new();

				v.push(f.clone());
				v.push(f.clone());

				let t = Tuple::new(v);
				one_candidate.push(t);
			}

			candiate_tuples.push(one_candidate);
		}
	}
	return candiate_tuples;
} 

//set up and call the grace hash join on the two set up tables
fn bench_grace_hashjoin(left : &Vec<Tuple>, right : &Vec<Tuple>) {
	let attr1 = Attribute::new("".to_string(), DataType::Int);
	let attr2 = Attribute::new("".to_string(), DataType::Int);

	let attributes1 = vec![attr1, attr2];
	let attributes2 = attributes1.clone();

	let schema1 = TableSchema::new(attributes1);
	let schema2 = TableSchema::new(attributes2);

	let left_child : Arc<Mutex<Box<dyn OpIterator + Send + Sync >>> =
		Arc::new(Mutex::new(Box::new(TupleIterator::new(left.clone(), schema1))));
	let right_child : Arc<Mutex<Box<dyn OpIterator + Send + Sync >>> =
		Arc::new(Mutex::new(Box::new(TupleIterator::new(right.clone(), schema2))));

	let mut grace_hashjoin = GraceHashEqJoin::new(common::SimplePredicateOp::Equals, 0, 0, left_child, right_child);
	grace_hashjoin.open();
}

//run the tests for the possible combinations of table sizes
pub fn grace_hashjoin_benchmark(c: &mut Criterion) {
	let candidate_tuples = get_random_tuple_iterators();
    c.bench_function("grace hashjoin tiny vs tiny", |b| {
        b.iter(|| bench_grace_hashjoin(black_box(&candidate_tuples[0]), black_box(&candidate_tuples[1])))
    });

	let candidate_tuples = get_random_tuple_iterators();
    c.bench_function("grace hashjoin tiny vs large", |b| {
        b.iter(|| bench_grace_hashjoin(black_box(&candidate_tuples[0]), black_box(&candidate_tuples[8])))
    });

	let candidate_tuples = get_random_tuple_iterators();
    c.bench_function("grace hashjoin large vs tiny", |b| {
        b.iter(|| bench_grace_hashjoin(black_box(&candidate_tuples[9]), black_box(&candidate_tuples[1])))
    });

	let candidate_tuples = get_random_tuple_iterators();
    c.bench_function("grace hashjoin large vs large", |b| {
        b.iter(|| bench_grace_hashjoin(black_box(&candidate_tuples[9]), black_box(&candidate_tuples[8])))
    });
}
