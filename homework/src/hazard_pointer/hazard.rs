use core::marker::PhantomData;
use core::ptr::{self, NonNull};
use std::collections::HashSet;
use std::fmt;

#[cfg(not(feature = "check-loom"))]
use core::sync::atomic::{fence, AtomicBool, AtomicPtr, AtomicUsize, Ordering};
#[cfg(feature = "check-loom")]
use loom::sync::atomic::{fence, AtomicBool, AtomicPtr, AtomicUsize, Ordering};

use super::HAZARDS;

/// Represents the ownership of a hazard pointer slot.
pub struct Shield<T> {
    slot: NonNull<HazardSlot>,
    _marker: PhantomData<*const T>, // !Send + !Sync
}

impl<T> Shield<T> {
    /// Creates a new shield for hazard pointer.
    pub fn new(hazards: &HazardBag) -> Self {
        let slot = hazards.acquire_slot();
        Self {
            slot: slot.into(),
            _marker: PhantomData,
        }
    }

    /// Try protecting the pointer `*pointer`.
    /// 1. Store `*pointer` to the hazard slot.
    /// 2. Check if `src` still points to `*pointer` (validation) and update `pointer` to the
    ///    latest value.
    /// 3. If validated, return true. Otherwise, clear the slot (store 0) and return false.
    pub fn try_protect(&self, pointer: &mut *const T, src: &AtomicPtr<T>) -> bool {
        unsafe {
            // 1. Store `*pointer` to the hazard slot.
            fence(Ordering::SeqCst);
            let ptr = *pointer;
            fence(Ordering::SeqCst);
            let slt = self.slot.as_ref();
            fence(Ordering::SeqCst);
            slt.hazard.store(ptr as usize, Ordering::Release);

            // 2. Check if `src` still points to `*pointer` (validation) and update `pointer` to the latest value.
            fence(Ordering::SeqCst);
            let source = src.load(Ordering::Acquire);

            // 3. If validated, return true.
            // Otherwise, clear the slot (store 0) and return false.
            fence(Ordering::SeqCst);
            if ptr == (source as *const T) {
                fence(Ordering::SeqCst);
                true
            } else {
                fence(Ordering::SeqCst);
                *pointer = source;
                fence(Ordering::SeqCst);
                slt.hazard.store(0, Ordering::Release);
                fence(Ordering::SeqCst);
                false
            }
        }
    }

    /// Get a protected pointer from `src`.
    pub fn protect(&self, src: &AtomicPtr<T>) -> *const T {
        let mut pointer = src.load(Ordering::Relaxed) as *const T;
        while !self.try_protect(&mut pointer, src) {
            #[cfg(feature = "check-loom")]
            loom::sync::atomic::spin_loop_hint();
        }
        pointer
    }
}

impl<T> Default for Shield<T> {
    fn default() -> Self {
        Self::new(&HAZARDS)
    }
}

impl<T> Drop for Shield<T> {
    /// Clear and release the ownership of the hazard slot.
    fn drop(&mut self) {
        unsafe {
            let slt = self.slot.as_ref();
            slt.hazard.store(0, Ordering::Release);
            slt.active.store(false, Ordering::Release);
        }
    }
}

impl<T> fmt::Debug for Shield<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shield")
            .field("slot address", &self.slot)
            .field("slot data", unsafe { self.slot.as_ref() })
            .finish()
    }
}

/// Global bag (multiset) of hazards pointers.
/// `HazardBag.head` and `HazardSlot.next` form a grow-only list of all hazard slots. Slots are
/// never removed from this list. Instead, it gets deactivated and recycled for other `Shield`s.
#[derive(Debug)]
pub struct HazardBag {
    head: AtomicPtr<HazardSlot>,
}

/// See `HazardBag`
#[derive(Debug)]
struct HazardSlot {
    // Whether this slot is occupied by a `Shield`.
    active: AtomicBool,
    // Machine representation of the hazard pointer.
    hazard: AtomicUsize,
    // Immutable pointer to the next slot in the bag.
    next: *const HazardSlot,
}

impl HazardSlot {
    fn new(next: *const HazardSlot) -> Self {
        HazardSlot {
            active: AtomicBool::new(true),
            hazard: AtomicUsize::new(0),
            next,
        }
    }
}

impl HazardBag {
    #[cfg(not(feature = "check-loom"))]
    /// Creates a new global hazard set.
    pub const fn new() -> Self {
        Self {
            head: AtomicPtr::new(ptr::null_mut()),
        }
    }

    #[cfg(feature = "check-loom")]
    /// Creates a new global hazard set.
    pub fn new() -> Self {
        Self {
            head: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Acquires a slot in the hazard set, either by recyling an inactive slot or allocating a new
    /// slot.
    fn acquire_slot(&self) -> &HazardSlot {
        if let Some(recycle_slot) = self.try_acquire_inactive() {
            return recycle_slot;
        }

        loop {
            let past_head = self.head.load(Ordering::Acquire);
            let new_hazard_slot = Box::into_raw(Box::new(HazardSlot::new(past_head)));
            unsafe {
                if self
                    .head
                    .compare_exchange(
                        past_head,
                        new_hazard_slot,
                        Ordering::Release,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    return &*new_hazard_slot;
                }
                drop(Box::from_raw(new_hazard_slot));
            }
        }
    }

    /// Find an inactive slot and activate it.
    fn try_acquire_inactive(&self) -> Option<&HazardSlot> {
        let mut node: *const HazardSlot = self.head.load(Ordering::Acquire);
        unsafe {
            while !node.is_null() {
                match node.as_ref().unwrap().active.compare_exchange(
                    false,
                    true,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        return Some(&*node);
                    }
                    Err(_) => {
                        node = (*node).next;
                    }
                }
            }
            None
        }
    }

    /// Returns all the hazards in the set.
    pub fn all_hazards(&self) -> HashSet<usize> {
        let mut hash_set: HashSet<usize> = HashSet::new();
        let mut node: *const HazardSlot = self.head.load(Ordering::Acquire);
        loop {
            if node.is_null() {
                return hash_set;
            }
            unsafe {
                let n = &*node;
                if n.active.load(Ordering::Acquire) {
                    let pointer = n.hazard.load(Ordering::Acquire);
                    hash_set.insert(pointer);
                }
                node = n.next as *const HazardSlot;
            }
        }
    }
}

impl Drop for HazardBag {
    /// Frees all slots.
    fn drop(&mut self) {
        unsafe {
            let mut node = self.head.load(Ordering::Acquire);

            while !node.is_null() {
                let next_node = (*node).next;
                drop(Box::from_raw(node));

                node = next_node as *mut HazardSlot;
            }
        }
    }
}

unsafe impl Send for HazardSlot {}
unsafe impl Sync for HazardSlot {}

#[cfg(all(test, not(feature = "check-loom")))]
mod tests {
    use super::{HazardBag, Shield};
    use std::collections::HashSet;
    use std::mem;
    use std::ops::Range;
    use std::sync::{atomic::AtomicPtr, Arc};
    use std::thread;

    const THREADS: usize = 8;
    const VALUES: Range<usize> = 1..1024;

    // `all_hazards` should return hazards protected by shield(s).
    #[test]
    fn all_hazards_protected() {
        let hazard_bag = Arc::new(HazardBag::new());
        let _ = (0..THREADS)
            .map(|_| {
                let hazard_bag = hazard_bag.clone();
                thread::spawn(move || {
                    for data in VALUES {
                        let src = AtomicPtr::new(data as *mut ());
                        let shield = Shield::new(&hazard_bag);
                        shield.protect(&src);
                        // leak the shield so that
                        mem::forget(shield);
                    }
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|th| th.join().unwrap())
            .collect::<Vec<_>>();
        let all = hazard_bag.all_hazards();
        let values = VALUES.collect();
        assert!(all.is_superset(&values))
    }

    // `all_hazards` should not return values that are no longer protected.
    #[test]
    fn all_hazards_unprotected() {
        let hazard_bag = Arc::new(HazardBag::new());
        let _ = (0..THREADS)
            .map(|_| {
                let hazard_bag = hazard_bag.clone();
                thread::spawn(move || {
                    for data in VALUES {
                        let src = AtomicPtr::new(data as *mut ());
                        let shield = Shield::new(&hazard_bag);
                        shield.protect(&src);
                    }
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|th| th.join().unwrap())
            .collect::<Vec<_>>();
        let all = hazard_bag.all_hazards();
        let values = VALUES.collect();
        let intersection: HashSet<_> = all.intersection(&values).collect();
        assert!(intersection.is_empty())
    }

    // `acquire_slot` should recycle existing slots.
    #[test]
    fn recycle_slots() {
        let hazard_bag = HazardBag::new();
        // allocate slots
        let shields = (0..1024)
            .map(|_| Shield::<()>::new(&hazard_bag))
            .collect::<Vec<_>>();
        // slot addresses
        let old_slots = shields
            .iter()
            .map(|s| s.slot.as_ptr() as usize)
            .collect::<HashSet<_>>();
        // release the slots
        drop(shields);

        let shields = (0..128)
            .map(|_| Shield::<()>::new(&hazard_bag))
            .collect::<Vec<_>>();
        let new_slots = shields
            .iter()
            .map(|s| s.slot.as_ptr() as usize)
            .collect::<HashSet<_>>();

        // no new slots should've been created
        assert!(new_slots.is_subset(&old_slots));
    }
}
