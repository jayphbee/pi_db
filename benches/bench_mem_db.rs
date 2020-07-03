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