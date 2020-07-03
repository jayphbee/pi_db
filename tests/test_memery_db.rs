#![feature(test)]
extern crate test;

use test::Bencher;

extern crate pi_db;

extern crate atom;
extern crate bon;
extern crate guid;
extern crate sinfo;

use std::sync::Arc;
use std::thread;
use std::time;

use atom::Atom;
use bon::WriteBuffer;
use guid::{Guid, GuidGen};
use sinfo::{EnumType, StructInfo};

use pi_db::db::{Bin, TabKV, TabMeta, Ware};
use pi_db::memery_db::DB;

use pi_db::mgr::Mgr;

#[bench]
fn test_iter_db(b: &mut Bencher) {
	let db = DB::new();
	let mgr = Mgr::new(GuidGen::new(0, 0));
	mgr.register(Atom::from("memory"), Arc::new(db));

	let tr  =mgr.transaction(true);

	let sinfo = Arc::new(TabMeta::new(
		EnumType::U32,
		EnumType::U32),
	);

	let mut items = vec![];
	for i in 0..20000 {
		let mut wb = WriteBuffer::new();
		wb.write_u32(i);
		items.push(TabKV {
			ware: Atom::from("memory"),
			tab: Atom::from("hello"),
			key: Arc::new(wb.get_byte().clone()),
			value: Some(Arc::new(wb.get_byte().clone())),
			index: i as usize,
		})
	}

	match tr.alter(&Atom::from("memory"), &Atom::from("hello"), Some(sinfo), Arc::new(|r|{})) {
		Some(_) => {
			match tr.prepare(Arc::new(|r|{})) {
				Some(_) => {
					tr.commit(Arc::new(|r|{}));
					let tr2 = mgr.transaction(true);
					tr2.modify(items, None, false, Arc::new(|r|{}));

					tr2.prepare(Arc::new(|r| {}));

					tr2.commit(Arc::new(|r|{}));

					b.iter(|| {
						// let start = std::time::Instant::now();
						let tr3 = mgr.transaction(false);
						
						let mut iter = tr3.iter(&Atom::from("memory"), &Atom::from("hello"), None, false, None, Arc::new(|r|{})).unwrap().unwrap();
	
						while let Some(Ok(Some(v))) = iter.next(Arc::new(|r|{})) {
						}
						// println!("iter time = {:?}", start.elapsed().as_micros());
					})
				}
				None => {}
			}
		}
		None => {

		}
	}

	thread::sleep(std::time::Duration::from_secs(10));
}

#[test]
fn test_memorydb_multi_thread() {
	let db = DB::new();
	let snapshot = db.snapshot();

	let sinfo = Arc::new(TabMeta::new(
		EnumType::Str,
		EnumType::Struct(Arc::new(StructInfo::new(Atom::from("test_table_2"), 8888))),
	));
	snapshot.alter(&Atom::from("test_table_2"), Some(sinfo.clone()));

	let meta_txn = snapshot.meta_txn(&Guid(0));

	assert!(meta_txn
		.alter(
			&Atom::from("test_table_2"),
			Some(sinfo.clone()),
			Arc::new(move |alter| {
				assert!(alter.is_ok());
			}),
		)
		.is_some());

	assert!(meta_txn
		.prepare(
			1000,
			Arc::new(move |p| {
				assert!(p.is_ok());
			}),
		)
		.is_some());

	thread::sleep(time::Duration::from_millis(1000));

	// commit success
	assert!(meta_txn
		.commit(Arc::new(move |c| {
			assert!(c.is_ok());
		}))
		.is_some());

	thread::sleep(time::Duration::from_millis(1000));

	assert!(snapshot.tab_info(&Atom::from("test_table_2")).is_some());

	let db_clone = Arc::into_raw(Arc::new(db.clone())) as usize;
	// concurrently insert 3000 items with 3 threads
	let h1 = thread::spawn(move || {
		let db1 = unsafe { Arc::from_raw(db_clone as *const pi_db::memery_db::DB) };
		let snapshot = db1.snapshot();

		assert!(snapshot.tab_info(&Atom::from("test_table_2")).is_some());

		let txn1 = snapshot
			.tab_txn(
				&Atom::from("test_table_2"),
				&Guid(0),
				true,
				Box::new(|_r| {}),
			)
			.unwrap()
			.unwrap();

		for i in 0..1000 {
			let k = build_db_key(&format!("test_key{:?}", i));
			let v = build_db_val(&format!("test_value{:?}", i));
			let item = create_tabkv(
				Atom::from("testdb"),
				Atom::from("test_table_2"),
				k.clone(),
				0,
				Some(v.clone()),
			);

			let m = txn1.modify(
				Arc::new(vec![item]),
				None,
				false,
				Arc::new(move |m| {
					assert!(m.is_ok());
				}),
			);
			assert!(m.is_some());
		}

		let p = txn1.prepare(
			1000,
			Arc::new(move |p| {
				assert!(p.is_ok());
			}),
		);
		assert!(p.is_some());

		let c = txn1.commit(Arc::new(move |c| {
			assert!(c.is_ok());
		}));
		assert!(c.is_some());
	})
	.join();

	thread::sleep(time::Duration::from_millis(2000));
}

fn create_tabkv(ware: Atom, tab: Atom, key: Bin, index: usize, value: Option<Bin>) -> TabKV {
	TabKV {
		ware,
		tab,
		key,
		index,
		value,
	}
}

fn build_db_key(key: &str) -> Arc<Vec<u8>> {
	let mut wb = WriteBuffer::new();
	wb.write_utf8(key);
	Arc::new(wb.get_byte().to_vec())
}

fn build_db_val(val: &str) -> Arc<Vec<u8>> {
	let mut wb = WriteBuffer::new();
	wb.write_utf8(val);
	Arc::new(wb.get_byte().to_vec())
}
