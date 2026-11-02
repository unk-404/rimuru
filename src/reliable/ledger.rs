//! Session-lifetime compressed terminal IDs. No clock-based replay reset.
use super::AbortReason;
use crate::{
    Error, Result,
    buffer::{Budget, BudgetAllocator},
};
use allocator_api2::vec::Vec;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Terminal {
    Complete,
    Abort(AbortReason),
}
#[derive(Clone, Copy)]
struct Interval {
    first: u32,
    last: u32,
    terminal: Terminal,
}
pub(super) struct Ledger {
    ranges: Vec<Interval, BudgetAllocator>,
}
pub(super) const LIMIT: usize = 4096;
impl Ledger {
    pub fn new(budget: Arc<Budget>) -> Self {
        Self {
            ranges: Vec::new_in(BudgetAllocator(budget)),
        }
    }
    /// Reserve one possible disjoint terminal interval for every admitted message.
    pub fn admit(&mut self, active: usize) -> Result<()> {
        if self.ranges.len() + active + 1 > LIMIT {
            return Err(Error::IdExhausted);
        }
        self.ranges
            .try_reserve(active + 1)
            .map_err(|_| Error::NoBuffer)
    }
    pub fn get(&self, id: u32) -> Option<Terminal> {
        let n = self.ranges.partition_point(|r| r.last < id);
        self.ranges
            .get(n)
            .filter(|r| r.first <= id)
            .map(|r| r.terminal)
    }
    pub fn insert(&mut self, id: u32, terminal: Terminal) {
        if self.get(id).is_some() {
            return;
        }
        let n = self.ranges.partition_point(|r| r.first < id);
        self.ranges.insert(
            n,
            Interval {
                first: id,
                last: id,
                terminal,
            },
        );
        let mut n = n;
        if n > 0
            && self.ranges[n - 1].terminal == terminal
            && self.ranges[n - 1].last.checked_add(1) == Some(id)
        {
            let end = self.ranges.remove(n).last;
            n -= 1;
            self.ranges[n].last = end;
        }
        if n + 1 < self.ranges.len()
            && self.ranges[n + 1].terminal == terminal
            && self.ranges[n].last.checked_add(1) == Some(self.ranges[n + 1].first)
        {
            let end = self.ranges.remove(n + 1).last;
            self.ranges[n].last = end;
        }
    }
}
