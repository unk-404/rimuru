use super::*;

#[test]
fn container_allocator_accounts_retained_storage_and_rejected_growth() {
    let parent = Budget::new(16384, None);
    let budget = Budget::new(8192, Some(parent.clone()));
    let mut map = hashbrown::HashMap::with_hasher_in(
        foldhash::fast::RandomState::default(),
        BudgetAllocator(budget.clone()),
    );
    assert_eq!(budget.used(), 0);
    for key in ["a", "b", "c", "d"] {
        map.try_reserve(1).unwrap();
        map.insert(key, [0x5au8; 512]);
        assert_eq!(budget.used(), map.allocation_size());
        assert_eq!(parent.used(), budget.used());
    }
    assert!(map.try_reserve(64).is_err());
    assert_eq!(budget.used(), map.allocation_size());
    assert_eq!(map["a"], [0x5a; 512]);
    map.clear();
    assert!(budget.used() > 0);
    map.shrink_to_fit();
    assert_eq!(budget.used(), 0);
    assert_eq!(parent.used(), 0);
}

#[test]
fn container_allocator_honors_alignment_and_releases_from_a_clone() {
    use allocator_api2::alloc::{Allocator, Layout};
    let budget = Budget::new(8192, None);
    let allocator = BudgetAllocator(budget.clone());
    let layout = Layout::from_size_align(128, 4096).unwrap();
    let block = allocator.allocate(layout).unwrap();
    assert_eq!(block.as_ptr().cast::<u8>() as usize % 4096, 0);
    assert_eq!(budget.used(), 128);
    // SAFETY: live block and identical layout, cloned allocator shares Global/ledger.
    unsafe {
        allocator.clone().deallocate(block.cast(), layout);
    }
    assert_eq!(budget.used(), 0);
}
#[test]
fn parent_rejection_rolls_back_and_leases_remain_charged() {
    let parent = Budget::new(1024, None);
    let child = Budget::new(2048, Some(parent.clone()));
    assert!(child.reserve(1025).is_err());
    assert_eq!(child.used(), 0);
    let pool = Pool::new(child.clone(), 512);
    let lease = pool.take(128).unwrap();
    assert!(child.used() >= 128);
    drop(lease);
    assert!(child.used() >= 128);
    pool.trim();
    assert_eq!(child.used(), 0);
    assert_eq!(parent.used(), 0);
}

#[test]
fn idle_trim_releases_only_expired_cached_blocks() {
    let budget = Budget::new(8192, None);
    let pool = Pool::new(budget.clone(), 4096);
    let old = pool.take(128).unwrap();
    let recent = pool.take(256).unwrap();
    drop(old);
    drop(recent);
    let now = Instant::now();
    pool.idle
        .lock()
        .unwrap()
        .iter_mut()
        .find(|item| item.data.capacity() == 128)
        .unwrap()
        .idle_since = now - Duration::from_secs(6);
    pool.trim_idle(now, Duration::from_secs(5));
    assert_eq!(pool.cached(), 256);
    assert_eq!(budget.used(), 256 + 64);
    pool.trim();
    assert_eq!(budget.used(), 0);
}
