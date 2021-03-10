use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use atom::Atom;
use bon::WriteBuffer;
use crossbeam_channel::bounded;
use guid::GuidGen;
use hash::XHashMap;
use pi_db::db::{TabKV, TabMeta};
use pi_db::log_file_db::{AsyncLogFileStore, LogFileDB, LOG_FILE_SIZE};
use pi_db::{
    log_file_db::{AsyncLogFileStoreInner, STORE_RUNTIME},
    mgr::{DatabaseWare, Mgr},
};
use r#async::{
    lock::spin_lock::SpinLock,
    rt::multi_thread::{MultiTaskPool, MultiTaskRuntime},
    rt::{AsyncMap, AsyncRuntime},
};
use sinfo;

#[test]
fn test_collect_log_file_db() {
    //初始化日志服务器
    env_logger::init();

    let pool = MultiTaskPool::new("Store-Runtime".to_string(), 8, 1024 * 1024, 10, Some(10));
    let rt: MultiTaskRuntime<()> = pool.startup(false);

    let rt_copy = rt.clone();
    let _ = rt.spawn(rt.alloc(), async move {
        *STORE_RUNTIME.write().await = Some(rt_copy.clone());
        LOG_FILE_SIZE.store(1, Ordering::SeqCst);

        let path = PathBuf::from("./tests/log");
        let file = match AsyncLogFileStore::open(path.clone(), 8000, 200 * 1024 * 1024, None).await
        {
            Err(e) => panic!("!!!!!!open table = {:?} failed, e: {:?}", path, e),
            Ok(file) => file,
        };

        let mut store = AsyncLogFileStore(Arc::new(AsyncLogFileStoreInner {
            removed: SpinLock::new(XHashMap::default()),
            map: SpinLock::new(BTreeMap::new()),
            log_file: file.clone(),
            tmp_map: SpinLock::new(XHashMap::default()),
            writable_path: SpinLock::new(None),
            is_statistics: AtomicBool::new(false),
            is_init: AtomicBool::new(true),
            statistics: SpinLock::new(VecDeque::new()),
        }));

        let _ = file.load(&mut store, Some(path.clone()), false).await;
        store.0.is_init.store(false, Ordering::SeqCst);

        let map_len = store.0.map.lock().len();
        let writable_path = store.0.writable_path.lock().as_ref().cloned();
        let is_statistics = store.0.is_statistics.load(Ordering::Relaxed);
        println!(
            "!!!!!!load ok, path: {:?}, map len: {}, writable_path: {:?}, is_statistics: {}",
            path, map_len, writable_path, is_statistics
        );

        let mut log_total_len = 0;
        for (log_file, log_len, key_len) in store.0.statistics.lock().iter() {
            log_total_len += log_len;
            println!(
                "!!!!!!load ok, file: {:?}, log len: {}, key len: {}",
                log_file, log_len, key_len
            );
        }
        println!("!!!!!!load finish, log total len: {}", log_total_len);

        println!("!!!!!!Init DB env");
        let mgr = Mgr::new(GuidGen::new(0, 0));
        let mgr_copy = mgr.clone();

        let ware = DatabaseWare::new_log_file_ware(
            LogFileDB::new(Atom::from("./tests/log"), 1024 * 1024 * 1024).await,
        );
        let _ = mgr_copy.register(Atom::from("./tests"), ware).await;

        let mut tr = mgr_copy.transaction(true, Some(rt_copy.clone())).await;

        let meta = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);

        let _ = tr
            .alter(
                &Atom::from("./tests"),
                &Atom::from("./tests/log"),
                Some(meta),
            )
            .await;
        let _ = tr.prepare().await;
        let _ = tr.commit().await;

        println!("!!!!!!Init Tab");
        let mut tr = mgr.transaction(true, Some(rt_copy.clone())).await;
        for index in 0..100000 {
            let mut items = vec![];

            let mut wb = WriteBuffer::new();
            let string = "Test".to_string() + index.to_string().as_str();
            let key = string.as_bytes();
            wb.write_bin(key, 0..key.len());

            items.push(TabKV {
                ware: Atom::from("./tests"),
                tab: Atom::from("./tests/log"),
                key: Arc::new(wb.bytes.clone()),
                value: Some(Arc::new(wb.bytes)),
                index: 0,
            });

            let _ = tr.modify(items, None, false).await;
        }
        let _ = tr.prepare().await;
        let _ = tr.commit().await;

        let mut tr = mgr.transaction(true, Some(rt_copy.clone())).await;
        for index in 0..100000i32 {
            let mut items = vec![];

            let mut wb = WriteBuffer::new();
            let string = "Test".to_string() + index.to_string().as_str();
            let key = string.as_bytes();
            wb.write_bin(key, 0..key.len());

            items.push(TabKV {
                ware: Atom::from("./tests"),
                tab: Atom::from("./tests/log"),
                key: Arc::new(wb.bytes.clone()),
                value: None,
                index: 0,
            });

            let _ = tr.modify(items, None, false).await;
        }
        let _ = tr.prepare().await;
        let _ = tr.commit().await;

        let mut tr = mgr.transaction(true, Some(rt_copy.clone())).await;
        for index in 0..100000i32 {
            let mut items = vec![];

            let mut wb = WriteBuffer::new();
            let string = "Test".to_string() + index.to_string().as_str();
            let key = string.as_bytes();
            wb.write_bin(key, 0..key.len());

            items.push(TabKV {
                ware: Atom::from("./tests"),
                tab: Atom::from("./tests/log"),
                key: Arc::new(wb.bytes.clone()),
                value: Some(Arc::new(wb.bytes)),
                index: 0,
            });

            let _ = tr.modify(items, None, false).await;
        }
        let _ = tr.prepare().await;
        let _ = tr.commit().await;

        let mut tr = mgr.transaction(true, Some(rt_copy.clone())).await;
        for index in 100000..200000i32 {
            let mut items = vec![];

            let mut wb = WriteBuffer::new();
            let string = "Test".to_string() + index.to_string().as_str();
            let key = string.as_bytes();
            wb.write_bin(key, 0..key.len());

            items.push(TabKV {
                ware: Atom::from("./tests"),
                tab: Atom::from("./tests/log"),
                key: Arc::new(wb.bytes.clone()),
                value: Some(Arc::new(wb.bytes)),
                index: 0,
            });

            let _ = tr.modify(items, None, false).await;
        }
        let _ = tr.prepare().await;
        let _ = tr.commit().await;

        let mut tr = mgr.transaction(true, Some(rt_copy.clone())).await;
        for index in 0..100000i32 {
            let mut items = vec![];

            let mut wb = WriteBuffer::new();
            let string = "Test".to_string() + index.to_string().as_str();
            let key = string.as_bytes();
            wb.write_bin(key, 0..key.len());

            items.push(TabKV {
                ware: Atom::from("./tests"),
                tab: Atom::from("./tests/log"),
                key: Arc::new(wb.bytes.clone()),
                value: Some(Arc::new(wb.bytes)),
                index: 0,
            });

            let _ = tr.modify(items, None, false).await;
        }
        let _ = tr.prepare().await;
        let _ = tr.commit().await;

        let mut tr = mgr.transaction(true, Some(rt_copy.clone())).await;
        for index in 100000..200000i32 {
            let mut items = vec![];

            let mut wb = WriteBuffer::new();
            let string = "Test".to_string() + index.to_string().as_str();
            let key = string.as_bytes();
            wb.write_bin(key, 0..key.len());

            items.push(TabKV {
                ware: Atom::from("./tests"),
                tab: Atom::from("./tests/log"),
                key: Arc::new(wb.bytes.clone()),
                value: Some(Arc::new(wb.bytes)),
                index: 0,
            });

            let _ = tr.modify(items, None, false).await;
        }
        let _ = tr.prepare().await;
        let _ = tr.commit().await;
        println!("!!!!!!Init Tab finish");

        rt_copy.wait_timeout(5000).await;

        println!("!!!!!!Test collect 0 start");
        if let Err(e) = LogFileDB::collect().await {
            panic!("Test collect failed, reason: {}", e);
        }
        println!("!!!!!!Test collect 0 finish");

        println!("!!!!!!Test collect 1 start");
        if let Err(e) = LogFileDB::collect().await {
            panic!("Test collect failed, reason: {}", e);
        }
        println!("!!!!!!Test collect 1 finish");
    });

    thread::sleep(Duration::from_millis(100000000));
}

#[test]
fn test_log_file_db() {
    let pool = MultiTaskPool::new("Store-Runtime".to_string(), 4, 1024 * 1024, 10, Some(10));
    let rt: MultiTaskRuntime<()> = pool.startup(true);
    let rt1 = rt.clone();

    let _ = rt1.spawn(rt.alloc(), async move {
        *STORE_RUNTIME.write().await = Some(rt.clone());
        let mgr = Mgr::new(GuidGen::new(0, 0));
        let ware = DatabaseWare::new_log_file_ware(
            LogFileDB::new(Atom::from("./testlogfile"), 1024 * 1024 * 1024).await,
        );
        let _ = mgr.register(Atom::from("logfile"), ware).await;
        let mut tr = mgr.transaction(true, Some(rt.clone())).await;

        let meta = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);
        let meta1 = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);

        let _ = tr
            .alter(
                &Atom::from("logfile"),
                &Atom::from("./testlogfile/hello"),
                Some(meta),
            )
            .await;
        let _ = tr
            .alter(
                &Atom::from("logfile"),
                &Atom::from("./testlogfile/world"),
                Some(meta1),
            )
            .await;
        let p = tr.prepare().await;
        println!("tr prepare ---- {:?}", p);
        let _ = tr.commit().await;

        let info = tr
            .tab_info(&Atom::from("logfile"), &Atom::from("./testlogfile/hello"))
            .await;
        println!("info ---- {:?} ", info);

        let mut wb = WriteBuffer::new();
        wb.write_bin(b"hello", 0..5);

        println!("wb = {:?}", wb.bytes);

        let mut item1 = TabKV {
            ware: Atom::from("logfile"),
            tab: Atom::from("./testlogfile/hello"),
            key: Arc::new(wb.bytes.clone()),
            value: Some(Arc::new(wb.bytes)),
            index: 0,
        };

        let mut writes = vec![];
        let key_template = "keyyyyyyyyyyyyyyyyyyyyyyyy".to_string();
        let value_template =
            "valuevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevalue"
                .to_string();

        for i in 0..100000 {
            let mut key_wb = WriteBuffer::new();
            let key = format!("{:?}{:?}", key_template, i);
            key_wb.write_bin(key.as_bytes(), 0..key.len());

            let mut value_wb = WriteBuffer::new();
            let value = format!("{:?}{:?}", value_template, i);
            value_wb.write_bin(value.as_bytes(), 0..value.len());
            writes.push(TabKV {
                ware: Atom::from("logfile"),
                tab: Atom::from("./testlogfile/hello"),
                key: Arc::new(key_wb.bytes.clone()),
                value: Some(Arc::new(value_wb.bytes.clone())),
                index: i,
            })
        }

        let mut tr2 = mgr.transaction(true, Some(rt.clone())).await;

        let r = tr2.modify(writes, None, false).await;
        println!("logfile result = {:?}", r);
        let _ = tr2.prepare().await;
        let _ = tr2.commit().await;

        let mut tr3 = mgr.transaction(false, Some(rt.clone())).await;
        item1.value = None;

        let q = tr3.query(vec![item1], None, false).await;
        println!("query item = {:?}", q);
        let _ = tr3.prepare().await;
        let _ = tr3.commit().await;

        let mut tr4 = mgr.transaction(false, Some(rt.clone())).await;
        let size = tr4
            .tab_size(&Atom::from("logfile"), &Atom::from("./testlogfile/hello"))
            .await;
        println!("tab size = {:?}", size);
        {
            let iter = tr4
                .iter(
                    &Atom::from("logfile"),
                    &Atom::from("./testlogfile/hello"),
                    None,
                    false,
                    None,
                )
                .await;

            if let Ok(mut it) = iter {
                loop {
                    let item = it.next();
                    println!("iter item = {:?}", item);
                    match item {
                        Some(Ok(None)) | Some(Err(_)) => break,
                        _ => {}
                    }
                }
            }
        }

        let tabs = tr4.list(&Atom::from("logfile")).await;
        println!("tabs = {:?}", tabs);

        let _ = tr4.prepare().await;
        let _ = tr4.commit().await;
    });

    std::thread::sleep(std::time::Duration::from_secs(20));
}

#[test]
fn write_test_data() {
    let pool = MultiTaskPool::new("Store-Runtime".to_string(), 4, 1024 * 1024, 10, Some(10));
    let rt: MultiTaskRuntime<()> = pool.startup(true);
    let rt1 = rt.clone();

    let _ = rt1.spawn(rt.alloc(), async move {
        *STORE_RUNTIME.write().await = Some(rt.clone());
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
                &Atom::from("./testlogfile/testtab"),
                Some(meta),
            )
            .await;
        let p = tr.prepare().await;
        println!("tr prepare ---- {:?}", p);
        let _ = tr.commit().await;

        let info = tr
            .tab_info(&Atom::from("logfile"), &Atom::from("./testlogfile/testtab"))
            .await;
        println!("info ---- {:?} ", info);

        let mut writes = vec![];
        let key_template = "keyyyyyyyyyyyyyyyyyyyyyyyy".to_string();
        let value_template =
            "valuevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevalue"
                .to_string();

        for i in 0..1000 {
            let mut key_wb = WriteBuffer::new();
            let key = format!("{:?}{:?}", key_template, i);
            key_wb.write_bin(key.as_bytes(), 0..key.len());

            let mut value_wb = WriteBuffer::new();
            let value = format!("{:?}{:?}", value_template, i);
            value_wb.write_bin(value.as_bytes(), 0..value.len());
            writes.push(TabKV {
                ware: Atom::from("logfile"),
                tab: Atom::from("./testlogfile/testtab"),
                key: Arc::new(key_wb.bytes.clone()),
                value: Some(Arc::new(value_wb.bytes.clone())),
                index: i,
            })
        }

        let mut tr2 = mgr.transaction(true, Some(rt.clone())).await;

        let r = tr2.modify(writes, None, false).await;
        println!("logfile result = {:?}", r);
        let p = tr2.prepare().await;
        println!("prepare result {:?}", p);
        let _ = tr2.commit().await;

        let mut tr4 = mgr.transaction(false, Some(rt.clone())).await;

        let size = tr4
            .tab_size(&Atom::from("logfile"), &Atom::from("./testlogfile/testtab"))
            .await;
        println!("tab size = {:?}", size);
        let tabs = tr4.list(&Atom::from("logfile")).await;
        println!("tabs = {:?}", tabs);

        let _ = tr4.prepare().await;
        let _ = tr4.commit().await;
    });

    std::thread::sleep(std::time::Duration::from_secs(200));
}

#[test]
fn read_test_data() {
    let pool = MultiTaskPool::new("Store-Runtime".to_string(), 4, 1024 * 1024, 10, Some(10));
    let rt: MultiTaskRuntime<()> = pool.startup(true);
    let rt1 = rt.clone();

    let _ = rt1.spawn(rt.alloc(), async move {
        *STORE_RUNTIME.write().await = Some(rt.clone());
        let mgr = Mgr::new(GuidGen::new(0, 0));
        let ware = DatabaseWare::new_log_file_ware(
            LogFileDB::new(Atom::from("./testlogfile"), 1024 * 1024 * 1024).await,
        );
        let _ = mgr.register(Atom::from("logfile"), ware).await;
        let mut tr = mgr.transaction(true, Some(rt.clone())).await;

        let meta = TabMeta::new(sinfo::EnumType::Str, sinfo::EnumType::Str);

        let a = tr
            .alter(
                &Atom::from("logfile"),
                &Atom::from("./testlogfile/testtab"),
                Some(meta),
            )
            .await;
        println!("alter result ==== {:?}", a);
        let p = tr.prepare().await;
        println!("tr prepare ---- {:?}", p);
        let _ = tr.commit().await;

        let info = tr
            .tab_info(&Atom::from("logfile"), &Atom::from("./testlogfile/testtab"))
            .await;
        println!("info ---- {:?} ", info);

        let mut tr4 = mgr.transaction(false, Some(rt.clone())).await;
        let size = tr4
            .tab_size(&Atom::from("logfile"), &Atom::from("./testlogfile/testtab"))
            .await;
        println!("tab size = {:?}", size);
        {
            let iter = tr4
                .iter(
                    &Atom::from("logfile"),
                    &Atom::from("./testlogfile/testtab"),
                    None,
                    false,
                    None,
                )
                .await;

            if let Ok(mut it) = iter {
                loop {
                    let item = it.next();
                    println!("iter item = {:?}", item);
                    match item {
                        Some(Ok(None)) | Some(Err(_)) => break,
                        _ => {}
                    }
                }
            }
        }
    });

    std::thread::sleep(std::time::Duration::from_secs(200));
}

#[test]
fn bench_log_file_write() {
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

    std::thread::sleep(std::time::Duration::from_millis(5000));

    let rt_copy = rt.clone();

    let rt_copy1 = rt_copy.clone();
    let mgr_copy = mgr.clone();

    let mut async_map: AsyncMap<(), ()> = rt.map();
    let (s, r) = bounded(1);
    let rt_copy = rt.clone();
    let _ = rt.spawn(rt_copy.alloc(), async move {
        let start = std::time::Instant::now();
        for index in 0..100 {
            let mgr_copy1 = mgr_copy.clone();
            let rt_copy2 = rt_copy1.clone();
            async_map.join(AsyncRuntime::Multi(rt_copy.clone()), async move {
                log_file_write(&rt_copy2, &mgr_copy1, index).await;
                Ok(())
            });
        }

        match async_map.map(AsyncRuntime::Multi(rt_copy.clone())).await {
            Ok(_) => {}
            Err(_) => {}
        }
        println!("total time ==== {:?}", start.elapsed());
        let _ = s.send(());
    });
    let _ = r.recv();
}

async fn log_file_write(rt: &MultiTaskRuntime<()>, mgr: &Mgr, index: usize) {
    let mut tr = mgr.transaction(true, Some(rt.clone())).await;
    let mut items = vec![];

    let mut wb = WriteBuffer::new();
    let string = "hello world".to_string() + index.to_string().as_str();
    let key = string.as_bytes();
    wb.write_bin(key, 0..key.len());

    items.push(TabKV {
        ware: Atom::from("logfile"),
        tab: Atom::from("./testlogfile/hello"),
        key: Arc::new(wb.bytes.clone()),
        value: Some(Arc::new(wb.bytes)),
        index: 0,
    });

    let _ = tr.modify(items, None, false).await;
    let _ = tr.prepare().await;
    let _ = tr.commit().await;
}
