//! Thread-safe key/value cache.

use std::borrow::{Borrow, BorrowMut};
use std::collections::hash_map::{self, HashMap};
use std::hash::Hash;
use std::sync::{Arc, RwLock};

/// Cache that remembers the result for each key.
#[derive(Debug, Default)]
pub struct Cache<K, V> {
    // todo! This is an example cache type. Build your own cache type that satisfies the
    // specification for `get_or_insert_with`.
    inner: RwLock<HashMap<K, Arc<Option<V>>>>,
}

impl<K: Eq + Hash + Clone, V: Clone> Cache<K, V> {
    /// Retrieve the value or insert a new one created by `f`.
    ///
    /// An invocation to this function should not block another invocation with a different key.
    /// For example, if a thread calls `get_or_insert_with(key1, f1)` and another thread calls
    /// `get_or_insert_with(key2, f2)` (`key1≠key2`, `key1,key2∉cache`) concurrently, `f1` and `f2`
    /// should run concurrently.
    ///
    /// On the other hand, since `f` may consume a lot of resource (= money), it's desirable not to
    /// duplicate the work. That is, `f` should be run only once for each key. Specifically, even
    /// for the concurrent invocations of `get_or_insert_with(key, f)`, `f` is called only once.
    ///
    /// Hint: the [`Entry`] API may be useful in implementing this function.
    ///
    /// [`Entry`]: https://doc.rust-lang.org/stable/std/collections/hash_map/struct.HashMap.html#method.entry
    pub fn get_or_insert_with<F: FnOnce(K) -> V>(&self, key: K, f: F) -> V {
        let read_hash_map = self.inner.read().unwrap();
        let value_in_map = read_hash_map.get(&key);
        match value_in_map {
            Some(val) => {
                let vall = val.borrow();
                match vall {
                    // 값이 잘 있음
                    Some(result) => result.clone(),
                    // None을 넣어둠 (아직 넣는 중임)
                    None => {
                        drop(read_hash_map);
                        loop {
                            let r_hash_map = self.inner.read().unwrap();
                            if let Some(value) = r_hash_map.get(&key) {
                                if let Some(value_final) = value.borrow() {
                                    return value_final.clone();
                                }
                            }
                            drop(r_hash_map);
                        }
                    }
                }
            }
            // 없어서 넣어야 함
            None => {
                // Read drop 후 넣을 값 더미 생성
                drop(read_hash_map);
                let value = Arc::new(None);

                // writelock 으로 받아온 hash map 에 더미 삽입 후 write lock 해제
                let mut write_hash_map = self.inner.write().unwrap();
                if write_hash_map.contains_key(&key) {
                    drop(write_hash_map);
                    loop {
                        let r_hash_map = self.inner.read().unwrap();
                        if let Some(value) = r_hash_map.get(&key) {
                            if let Some(value_final) = value.borrow() {
                                return value_final.clone();
                            }
                        }
                        drop(r_hash_map);
                    }
                } else {
                    write_hash_map.insert(key.clone(), Arc::clone(&value));
                    drop(write_hash_map);

                    // Result 계산 후 더미 레퍼런스에 집어넣기
                    let result = f(key.clone());
                    let mut write_hash_map = self.inner.write().unwrap();
                    *write_hash_map.get_mut(&key).unwrap() = Arc::new(Some(result.clone()));
                    drop(write_hash_map);
                    result
                }
            }
        }
    }
}
