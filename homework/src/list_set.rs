use std::cmp;
use std::mem;
use std::ptr;
use std::sync::{Mutex, MutexGuard};

#[derive(Debug)]
struct Node<T> {
    data: T,
    next: Mutex<*mut Node<T>>,
}

unsafe impl<T: Send> Send for Node<T> {}
unsafe impl<T: Sync> Sync for Node<T> {}

/// Concurrent sorted singly linked list using lock-coupling.
#[derive(Debug)]
pub struct OrderedListSet<T> {
    head: Mutex<*mut Node<T>>,
}

unsafe impl<T: Send> Send for OrderedListSet<T> {}
unsafe impl<T: Sync> Sync for OrderedListSet<T> {}

// reference to the `next` field of previous node which points to the current node
// 직전 Node에서의 "next" 를 reference 한다.
struct Cursor<'l, T>(MutexGuard<'l, *mut Node<T>>);

impl<T> Node<T> {
    fn new(data: T, next: *mut Self) -> *mut Self {
        Box::into_raw(Box::new(Self {
            data,
            next: Mutex::new(next),
        }))
    }
}

impl<'l, T: Ord> Cursor<'l, T> {
    /// Move the cursor to the position of key in the sorted list. If the key is found in the list,
    /// return `true`.
    fn find(&mut self, key: &T) -> bool {
        let mut curr_node = *self.0;
        unsafe {
            loop {
                if curr_node.is_null() || (*curr_node).data > *key {
                    return false;
                } else if (*curr_node).data.eq(key) {
                    return true;
                } else {
                    let next_node = (*curr_node).next.lock().unwrap();
                    *self = Cursor(next_node);
                    curr_node = *self.0;
                }
            }
        }
    }
}

impl<T> OrderedListSet<T> {
    /// Creates a new list.
    pub fn new() -> Self {
        Self {
            head: Mutex::new(ptr::null_mut()),
        }
    }
}

impl<T: Ord> OrderedListSet<T> {
    fn find(&self, key: &T) -> (bool, Cursor<T>) {
        let mut find_cursor = Cursor(self.head.lock().unwrap());
        let result = find_cursor.find(key);
        (result, find_cursor)
    }

    /// Returns `true` if the set contains the key.
    pub fn contains(&self, key: &T) -> bool {
        let mut find_cursor = Cursor(self.head.lock().unwrap());
        find_cursor.find(key)
    }

    /// Insert a key to the set. If the set already has the key, return the provided key in `Err`.
    pub fn insert(&self, key: T) -> Result<(), T> {
        let (result, mut find_cursor) = self.find(&key);
        if result {
            Err(key)
        } else {
            let new_node = Node::new(key, *find_cursor.0);
            *find_cursor.0 = new_node;
            Ok(())
        }
    }

    /// Remove the key from the set and return it.
    pub fn remove(&self, key: &T) -> Result<T, ()> {
        let mut cursor = Cursor(self.head.lock().unwrap());
        if cursor.find(key) {
            unsafe {
                let curr_node = *cursor.0;
                let next_node = *(*curr_node).next.lock().unwrap();
                *cursor.0 = next_node;
                Ok(Box::from_raw(curr_node).data)
            }
        } else {
            Err(())
        }
    }
}

#[derive(Debug)]
pub struct Iter<'l, T>(Option<MutexGuard<'l, *mut Node<T>>>);

impl<T> OrderedListSet<T> {
    /// An iterator visiting all elements.
    pub fn iter(&self) -> Iter<T> {
        Iter(Some(self.head.lock().unwrap()))
    }
}

impl<'l, T> Iterator for Iter<'l, T> {
    type Item = &'l T;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.as_ref() {
            Some(guard) => {
                let node = **guard;
                if node.is_null() {
                    *self = Iter(None);
                    None
                } else {
                    unsafe {
                        let next_node = (*node).next.lock().unwrap();
                        *self = Iter(Some(next_node));
                        Some(&(*node).data)
                    }
                }
            }
            None => None,
        }
    }
}

impl<T> Drop for OrderedListSet<T> {
    fn drop(&mut self) {
        let mut curr_node = *self.head.get_mut().unwrap();
        unsafe {
            loop {
                if curr_node.is_null() {
                    return;
                } else {
                    let next_node = *(*curr_node).next.lock().unwrap();
                    drop(Box::from_raw(curr_node));
                    curr_node = next_node;
                }
            }
        }
    }
}

impl<T> Default for OrderedListSet<T> {
    fn default() -> Self {
        Self::new()
    }
}
