//! One active frame stays inline; interleaved frames spill into hashbrown.
use super::assembly::Assembly as Partial;
use crate::{
    Error, Result,
    buffer::{Budget, BudgetAllocator},
};
use hashbrown::HashMap;
use std::{
    hash::{BuildHasherDefault, Hasher},
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(Default)]
struct Direct(u64);
impl Hasher for Direct {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write_u32(&mut self, value: u32) {
        self.0 = u64::from(value);
    }
    fn write(&mut self, _: &[u8]) {
        unreachable!("private u32-only map");
    }
}
pub(super) struct Contexts {
    hot: Option<(u32, Partial)>,
    cold: HashMap<u32, Partial, BuildHasherDefault<Direct>, BudgetAllocator>,
}
impl Contexts {
    pub fn new(budget: Arc<Budget>) -> Self {
        Self {
            hot: None,
            cold: HashMap::with_hasher_in(BuildHasherDefault::default(), BudgetAllocator(budget)),
        }
    }
    #[inline]
    pub fn get(&self, id: u32) -> Option<&Partial> {
        if let Some((current, partial)) = &self.hot
            && *current == id
        {
            return Some(partial);
        }
        self.cold.get(&id)
    }
    #[inline]
    pub fn get_mut(&mut self, id: u32) -> Option<&mut Partial> {
        if let Some((current, partial)) = &mut self.hot
            && *current == id
        {
            return Some(partial);
        }
        self.cold.get_mut(&id)
    }
    pub fn insert(&mut self, id: u32, partial: Partial) -> Result<()> {
        // Failure leaves accepted work intact; reserve before moving hot state.
        if self.hot.is_some() {
            self.cold.try_reserve(1).map_err(|_| Error::NoBuffer)?;
        }
        if let Some((old_id, old)) = self.hot.replace((id, partial)) {
            self.cold.insert(old_id, old);
        }
        Ok(())
    }
    pub fn remove(&mut self, id: u32) -> Option<Partial> {
        if self.hot.as_ref().is_some_and(|(current, _)| *current == id) {
            return self.hot.take().map(|(_, partial)| partial);
        }
        let partial = self.cold.remove(&id);
        if self.cold.is_empty() {
            self.cold.shrink_to_fit();
        }
        partial
    }
    pub fn expire(&mut self, now: Instant, timeout: Duration) -> usize {
        let before = self.cold.len() + usize::from(self.hot.is_some());
        if self
            .hot
            .as_ref()
            .is_some_and(|(_, p)| now.duration_since(p.started) >= timeout)
        {
            self.hot = None;
        }
        self.cold
            .retain(|_, p| now.duration_since(p.started) < timeout);
        if self.cold.is_empty() {
            self.cold.shrink_to_fit();
        }
        before - self.cold.len() - usize::from(self.hot.is_some())
    }
}
