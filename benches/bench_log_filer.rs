#![feature(test)]

extern crate test;
use test::Bencher;

use std::sync::Arc;

use atom::Atom;
use bon::WriteBuffer;
use guid::GuidGen;
use pi_db::log_file_db::LogFileDB;
use pi_db::mgr::{DatabaseWare, Mgr};
use pi_db::{
    db::{TabKV, TabMeta},
    mgr::Tr,
};
use r#async::rt::multi_thread::{MultiTaskPool, MultiTaskRuntime};
use r#async::rt::AsyncRuntime;
use sinfo;

use crossbeam_channel::bounded;
use pi_db::log_file_db::STORE_RUNTIME;
use std::time::Duration;

#[bench]
fn bench_log_file_alter_tab(b: &mut Bencher) {
    let pool = MultiTaskPool::new("Store-Runtime".to_string(), 4, 1024 * 1024, 10, Some(10));
    let rt: MultiTaskRuntime<()> = pool.startup(true);

    b.iter(|| {
        let (s, r) = bounded(1);
        let rt1 = rt.clone();
        let _ = rt.spawn(rt.alloc(), async move {
            if STORE_RUNTIME.read().await.is_none() {
                *STORE_RUNTIME.write().await = Some(rt1.clone());
            }
            log_file_alter_tab(rt1).await;
            let _ = s.send(());
        });
        let _ = r.recv();
    })
}

#[bench]
fn bench_log_file_iter_tab(b: &mut Bencher) {
    let pool = MultiTaskPool::new("Store-Runtime".to_string(), 4, 1024 * 1024, 10, Some(10));
    let rt: MultiTaskRuntime<()> = pool.startup(true);

    b.iter(|| {
        let (s, r) = bounded(1);
        let rt1 = rt.clone();
        let _ = rt.spawn(rt.alloc(), async move {
            if STORE_RUNTIME.read().await.is_none() {
                *STORE_RUNTIME.write().await = Some(rt1.clone());
            }
            log_file_iter_tab(rt1).await;
            let _ = s.send(());
        });
        let _ = r.recv();
    })
}

#[bench]
fn bench_log_file_write(b: &mut Bencher) {
    let pool = MultiTaskPool::new("Store-Runtime".to_string(), 4, 1024 * 1024, 10, Some(10));
    let rt: MultiTaskRuntime<()> = pool.startup(false);

    let mgr = Mgr::new(GuidGen::new(0, 0));
    let mgr_copy = mgr.clone();

    let rt1 = rt.clone();
    let _ = rt.spawn(rt.alloc(), async move {
        if STORE_RUNTIME.read().await.is_none() {
            *STORE_RUNTIME.write().await = Some(rt1.clone());
        }

        let ware = DatabaseWare::new_log_file_ware(
            LogFileDB::new(Atom::from("./testlogfile"), 1024 * 1024 * 1024).await,
        );
        let _ = mgr_copy.register(Atom::from("logfile"), ware).await;

        let mut tr = mgr_copy.transaction(true, Some(rt1.clone())).await;

        let meta = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);

        let _ = tr
            .alter(
                &Atom::from("logfile"),
                &Atom::from("./testlogfile/hello"),
                Some(meta),
            )
            .await;
        let _ = tr.prepare().await;
        let _ = tr.commit().await;
    });

    std::thread::sleep(Duration::from_millis(5000));

    let mut item = vec![];
    let mut wb = WriteBuffer::new();
    let key = b"hello world1";
    wb.write_bin(key, 0..key.len());

    item.push(TabKV {
        ware: Atom::from("logfile"),
        tab: Atom::from("./testlogfile/hello"),
        key: Arc::new(wb.bytes.clone()),
        value: Some(Arc::new(wb.bytes)),
        index: 0,
    });

    let rt_copy = rt.clone();
    b.iter(|| {
        let rt_copy1 = rt_copy.clone();
        let mgr_copy = mgr.clone();
        let item = item.clone();

        let (s, r) = bounded(1);
        let _ = rt.spawn(rt.alloc(), async move {
            // for index in 0..1000 {
            log_file_write(&rt_copy1, &mgr_copy, item.clone()).await;
            // }
            let _ = s.send(());
        });
        let _ = r.recv();
    });
}

#[bench]
fn bench_log_file_read(b: &mut Bencher) {
    let pool = MultiTaskPool::new("Store-Runtime".to_string(), 4, 1024 * 1024, 10, Some(10));
    let rt: MultiTaskRuntime<()> = pool.startup(false);

    let mgr = Mgr::new(GuidGen::new(0, 0));
    let mgr_copy = mgr.clone();

    let rt1 = rt.clone();
    let _ = rt.spawn(rt.alloc(), async move {
        if STORE_RUNTIME.read().await.is_none() {
            *STORE_RUNTIME.write().await = Some(rt1.clone());
        }

        let ware = DatabaseWare::new_log_file_ware(
            LogFileDB::new(Atom::from("./testlogfile"), 1024 * 1024 * 1024).await,
        );
        let _ = mgr_copy.register(Atom::from("logfile"), ware).await;

        let mut tr = mgr_copy.transaction(true, Some(rt1.clone())).await;

        let meta = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);

        let _ = tr
            .alter(
                &Atom::from("logfile"),
                &Atom::from("./testlogfile/hello"),
                Some(meta),
            )
            .await;
        let _ = tr.prepare().await;
        let _ = tr.commit().await;
    });

    std::thread::sleep(Duration::from_millis(5000));

    let rt_copy = rt.clone();
    b.iter(|| {
        let rt_copy1 = rt_copy.clone();
        let mgr_copy = mgr.clone();

        let (s, r) = bounded(1);
        let _ = rt_copy.spawn(rt_copy.alloc(), async move {
            for _ in 0..1000 {
                log_file_read(&rt_copy1, &mgr_copy).await;
            }
            let _ = s.send(());
        });
        let _ = r.recv();
    });
}

async fn create_tab(tr: &mut Tr, tab_name: Atom, meta: TabMeta) {
    let _ = tr
        .alter(&Atom::from("logfile"), &tab_name, Some(meta.clone()))
        .await;
    let _ = tr.prepare().await;
    let _ = tr.commit().await;
}

#[bench]
fn bench_file_db_concurrent_write(b: &mut Bencher) {
    let pool = MultiTaskPool::new("Store-Runtime".to_string(), 4, 1024 * 1024, 10, Some(10));
    let rt: MultiTaskRuntime<()> = pool.startup(true);

    let mgr = Mgr::new(GuidGen::new(0, 0));
    let mgr_copy = mgr.clone();

    let rt1 = rt.clone();
    let _ = rt.spawn(rt.alloc(), async move {
        if STORE_RUNTIME.read().await.is_none() {
            *STORE_RUNTIME.write().await = Some(rt1.clone());
        }

        let ware = DatabaseWare::new_log_file_ware(
            LogFileDB::new(Atom::from("./testlogfile"), 1024 * 1024 * 1024).await,
        );
        let _ = mgr_copy.register(Atom::from("logfile"), ware).await;

        for i in 0..5 {
            let mut tr = mgr_copy.transaction(true, Some(rt1.clone())).await;
            let meta = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);
            create_tab(
                &mut tr,
                Atom::from(format!("./testlogfile/hello{}", i)),
                meta,
            )
            .await;
        }
    });

    std::thread::sleep(Duration::from_millis(5000));

    b.iter(|| {
        let (s, r) = bounded(1);
        let rt1 = rt.clone();
        let mgr_copy = mgr.clone();
        let _ = rt.spawn(rt.alloc(), async move {
            test_log_file_db_concurrent_write(&mgr_copy, rt1.clone()).await;
            let _ = s.send(());
        });
        let _ = r.recv();
    });
}

#[bench]
fn bench_file_db_concurrent_read(b: &mut Bencher) {
    let pool = MultiTaskPool::new("Store-Runtime".to_string(), 4, 1024 * 1024, 10, Some(10));
    let rt: MultiTaskRuntime<()> = pool.startup(true);
    let mgr = Mgr::new(GuidGen::new(0, 0));

    b.iter(|| {
        let (s, r) = bounded(1);
        let rt1 = rt.clone();
        let mgr1 = mgr.clone();
        let _ = rt.spawn(rt.alloc(), async move {
            if STORE_RUNTIME.read().await.is_none() {
                *STORE_RUNTIME.write().await = Some(rt1.clone());
            }
            test_log_file_db_concurrent_read(rt1, mgr1).await;
            let _ = s.send(());
        });
        let _ = r.recv();
    });
}

async fn test_log_file_db_concurrent_read(rt: MultiTaskRuntime<()>, mgr: Mgr) {
    let ware = DatabaseWare::new_log_file_ware(
        LogFileDB::new(Atom::from("./testlogfile"), 1024 * 1024 * 1024).await,
    );
    let _ = mgr.register(Atom::from("logfile"), ware).await;

    // let _ = tr.alter(
    //     &Atom::from("logfile"),
    //     &Atom::from("./testlogfile/hello"),
    //     Some(meta),
    // )
    // .await;
    // let _ = tr.prepare().await;
    // let _ = tr.commit().await;

    let mgr2 = mgr.clone();
    let mgr3 = mgr.clone();
    let mgr5 = mgr.clone();
    let mgr4 = mgr.clone();
    let mgr6 = mgr.clone();

    let mut map = rt.map();
    let rt1 = rt.clone();
    let rt2 = rt.clone();
    let rt3 = rt.clone();
    let rt4 = rt.clone();
    let rt5 = rt.clone();

    async move {
        map.join(AsyncRuntime::Multi(rt1.clone()), async move {
            let mut items = vec![];
            let mut wb = WriteBuffer::new();
            let key = b"hello world1";
            wb.write_bin(key, 0..key.len());

            items.push(TabKV {
                ware: Atom::from("logfile"),
                tab: Atom::from("./testlogfile/hello"),
                key: Arc::new(wb.bytes.clone()),
                value: Some(Arc::new(wb.bytes)),
                index: 0,
            });

            let mut tr2 = mgr2.transaction(true, Some(rt1.clone())).await;

            let _ = tr2.query(items, None, false).await;
            let _ = tr2.prepare().await;
            let _ = tr2.commit().await;
            Ok(())
        });

        map.join(AsyncRuntime::Multi(rt.clone()), async move {
            let mut items = vec![];
            let mut wb = WriteBuffer::new();
            let key = b"hello world2";
            wb.write_bin(key, 0..key.len());

            items.push(TabKV {
                ware: Atom::from("logfile"),
                tab: Atom::from("./testlogfile/hello"),
                key: Arc::new(wb.bytes.clone()),
                value: Some(Arc::new(wb.bytes)),
                index: 0,
            });

            let mut tr2 = mgr3.transaction(true, Some(rt.clone())).await;

            let _ = tr2.query(items, None, false).await;
            let _ = tr2.prepare().await;
            let _ = tr2.commit().await;
            Ok(())
        });

        map.join(AsyncRuntime::Multi(rt2.clone()), async move {
            let mut items = vec![];
            let mut wb = WriteBuffer::new();
            let key = b"hello world3";
            wb.write_bin(key, 0..key.len());

            items.push(TabKV {
                ware: Atom::from("logfile"),
                tab: Atom::from("./testlogfile/hello"),
                key: Arc::new(wb.bytes.clone()),
                value: Some(Arc::new(wb.bytes)),
                index: 0,
            });

            let mut tr2 = mgr4.transaction(true, Some(rt2.clone())).await;

            let _ = tr2.query(items, None, false).await;
            let _ = tr2.prepare().await;
            let _ = tr2.commit().await;
            Ok(())
        });

        map.join(AsyncRuntime::Multi(rt3.clone()), async move {
            let mut items = vec![];
            let mut wb = WriteBuffer::new();
            let key = b"hello world4";
            wb.write_bin(key, 0..key.len());

            items.push(TabKV {
                ware: Atom::from("logfile"),
                tab: Atom::from("./testlogfile/hello"),
                key: Arc::new(wb.bytes.clone()),
                value: Some(Arc::new(wb.bytes)),
                index: 0,
            });

            let mut tr2 = mgr5.transaction(true, Some(rt3.clone())).await;

            let _ = tr2.query(items, None, false).await;
            let _ = tr2.prepare().await;
            let _ = tr2.commit().await;
            Ok(())
        });

        map.join(AsyncRuntime::Multi(rt4.clone()), async move {
            let mut items = vec![];
            let mut wb = WriteBuffer::new();
            let key = b"hello world5";
            wb.write_bin(key, 0..key.len());

            items.push(TabKV {
                ware: Atom::from("logfile"),
                tab: Atom::from("./testlogfile/hello"),
                key: Arc::new(wb.bytes.clone()),
                value: Some(Arc::new(wb.bytes)),
                index: 0,
            });

            let mut tr2 = mgr6.transaction(true, Some(rt4.clone())).await;

            let _ = tr2.query(items, None, false).await;
            let _ = tr2.prepare().await;
            let _ = tr2.commit().await;
            Ok(())
        });
        let _ = map.map(AsyncRuntime::Multi(rt5.clone())).await;
    }
    .await
}

async fn test_log_file_db_concurrent_write(mgr: &Mgr, rt: MultiTaskRuntime<()>) {
    let mgr2 = mgr.clone();
    let mgr3 = mgr.clone();
    let mgr5 = mgr.clone();
    let mgr4 = mgr.clone();
    let mgr6 = mgr.clone();

    let mut map = rt.map();
    let rt1 = rt.clone();
    let rt2 = rt.clone();
    let rt3 = rt.clone();
    let rt4 = rt.clone();
    let rt5 = rt.clone();
    let rt6 = rt.clone();

    async move {
        map.join(AsyncRuntime::Multi(rt1.clone()), async move {
            let mut items = vec![];
            let mut wb = WriteBuffer::new();
            let key = b"hello world1";
            wb.write_bin(key, 0..key.len());

            items.push(TabKV {
                ware: Atom::from("logfile"),
                tab: Atom::from("./testlogfile/hello0"),
                key: Arc::new(wb.bytes.clone()),
                value: Some(Arc::new(wb.bytes)),
                index: 0,
            });

            let mut tr2 = mgr2.transaction(true, Some(rt1.clone())).await;

            let _ = tr2.modify(items, None, false).await;
            let _ = tr2.prepare().await;
            let _ = tr2.commit().await;
            Ok(())
        });

        map.join(AsyncRuntime::Multi(rt2.clone()), async move {
            let mut items = vec![];
            let mut wb = WriteBuffer::new();
            let key = b"hello world2";
            wb.write_bin(key, 0..key.len());

            items.push(TabKV {
                ware: Atom::from("logfile"),
                tab: Atom::from("./testlogfile/hello1"),
                key: Arc::new(wb.bytes.clone()),
                value: Some(Arc::new(wb.bytes)),
                index: 0,
            });

            let mut tr2 = mgr3.transaction(true, Some(rt2.clone())).await;

            let _ = tr2.modify(items, None, false).await;
            let _ = tr2.prepare().await;
            let _ = tr2.commit().await;
            Ok(())
        });

        map.join(AsyncRuntime::Multi(rt3.clone()), async move {
            let mut items = vec![];
            let mut wb = WriteBuffer::new();
            let key = b"hello world3";
            wb.write_bin(key, 0..key.len());

            items.push(TabKV {
                ware: Atom::from("logfile"),
                tab: Atom::from("./testlogfile/hello2"),
                key: Arc::new(wb.bytes.clone()),
                value: Some(Arc::new(wb.bytes)),
                index: 0,
            });

            let mut tr2 = mgr4.transaction(true, Some(rt3.clone())).await;

            let _ = tr2.modify(items, None, false).await;
            let _ = tr2.prepare().await;
            let _ = tr2.commit().await;
            Ok(())
        });

        map.join(AsyncRuntime::Multi(rt4.clone()), async move {
            let mut items = vec![];
            let mut wb = WriteBuffer::new();
            let key = b"hello world4";
            wb.write_bin(key, 0..key.len());

            items.push(TabKV {
                ware: Atom::from("logfile"),
                tab: Atom::from("./testlogfile/hello3"),
                key: Arc::new(wb.bytes.clone()),
                value: Some(Arc::new(wb.bytes)),
                index: 0,
            });

            let mut tr2 = mgr5.transaction(true, Some(rt4.clone())).await;

            let _ = tr2.modify(items, None, false).await;
            let _ = tr2.prepare().await;
            let _ = tr2.commit().await;
            Ok(())
        });

        map.join(AsyncRuntime::Multi(rt5.clone()), async move {
            let mut items = vec![];
            let mut wb = WriteBuffer::new();
            let key = b"hello world5";
            wb.write_bin(key, 0..key.len());

            items.push(TabKV {
                ware: Atom::from("logfile"),
                tab: Atom::from("./testlogfile/hello4"),
                key: Arc::new(wb.bytes.clone()),
                value: Some(Arc::new(wb.bytes)),
                index: 0,
            });

            let mut tr2 = mgr6.transaction(true, Some(rt5.clone())).await;

            let _ = tr2.modify(items, None, false).await;
            let _ = tr2.prepare().await;
            let _ = tr2.commit().await;
            Ok(())
        });
        let _ = map.map(AsyncRuntime::Multi(rt6.clone())).await;
    }
    .await
}

async fn log_file_read(rt: &MultiTaskRuntime<()>, mgr: &Mgr) {
    let mut tr = mgr.transaction(true, Some(rt.clone())).await;

    let mut wb = WriteBuffer::new();
    wb.write_bin(b"hello0", 0..6);

    let item1 = TabKV {
        ware: Atom::from("logfile"),
        tab: Atom::from("./testlogfile/hello"),
        key: Arc::new(wb.bytes.clone()),
        value: None,
        index: 0,
    };

    let _ = tr.query(vec![item1], None, false).await;
    let _ = tr.prepare().await;
    let _ = tr.commit().await;
}

async fn log_file_write(rt: &MultiTaskRuntime<()>, mgr: &Mgr, item: Vec<TabKV>) {
    let mut tr = mgr.transaction(true, Some(rt.clone())).await;
    let _ = tr.modify(item, None, false).await;
    let _ = tr.prepare().await;
    let _ = tr.commit().await;
}

async fn log_file_alter_tab(rt: MultiTaskRuntime<()>) {
    let mgr = Mgr::new(GuidGen::new(0, 0));
    let ware = DatabaseWare::new_log_file_ware(
        LogFileDB::new(Atom::from("./testlogfile"), 1024 * 1024 * 1024).await,
    );
    let _ = mgr.register(Atom::from("logfile"), ware).await;
    let mut tr = mgr.transaction(true, Some(rt.clone())).await;

    let meta = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);

    let _ = tr
        .alter(
            &Atom::from("logfile"),
            &Atom::from("./testlogfile/hello"),
            Some(meta),
        )
        .await;
    let _ = tr.prepare().await;
    let _ = tr.commit().await;
}

async fn log_file_iter_tab(rt: MultiTaskRuntime<()>) {
    let mgr = Mgr::new(GuidGen::new(0, 0));
    let ware = DatabaseWare::new_log_file_ware(
        LogFileDB::new(Atom::from("./testlogfile"), 1024 * 1024 * 1024).await,
    );
    let _ = mgr.register(Atom::from("logfile"), ware).await;
    // let mut tr = mgr.transaction(true, Some(rt.clone())).await;

    // let meta = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);

    // let _ = let _ = tr.alter(
    //     &Atom::from("logfile"),
    //     &Atom::from("./testlogfile/hello"),
    //     Some(meta),
    // )
    // .await;
    // let _ = let _ = tr.prepare().await;
    // let _ = let _ = tr.commit().await;

    let mut items = vec![];

    let mut wb = WriteBuffer::new();
    let key = b"hello world";
    wb.write_bin(key, 0..key.len());

    items.push(TabKV {
        ware: Atom::from("logfile"),
        tab: Atom::from("./testlogfile/hello"),
        key: Arc::new(wb.bytes.clone()),
        value: Some(Arc::new(wb.bytes)),
        index: 0,
    });

    let mut tr2 = mgr.transaction(true, Some(rt.clone())).await;

    let mut it = tr2
        .iter(
            &Atom::from("logfile"),
            &Atom::from("./testlogfile/hello"),
            None,
            true,
            None,
        )
        .await
        .unwrap();
    while let Some(x) = it.next() {
        if x.unwrap().is_none() {
            break;
        }
    }

    let _ = tr2.prepare().await;
    let _ = tr2.commit().await;
}
