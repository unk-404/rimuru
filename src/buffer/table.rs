use crate::buffer::{Budget, BudgetAllocator};
use std::{
    hash::{BuildHasherDefault, Hasher},
    sync::Arc,
};
#[derive(Default)]
pub(crate) struct Direct(u64);
impl Hasher for Direct {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write_u32(&mut self, value: u32) {
        self.0 = u64::from(value);
    }
    fn write(&mut self, _: &[u8]) {
        unreachable!("u32 keys only")
    }
}
pub(crate) type Map<V> = hashbrown::HashMap<u32, V, BuildHasherDefault<Direct>, BudgetAllocator>;
pub(crate) fn new<V>(budget: Arc<Budget>) -> Map<V> {
    Map::with_hasher_in(BuildHasherDefault::default(), BudgetAllocator(budget))
}
