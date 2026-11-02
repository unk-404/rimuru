//! Account container allocations while delegating storage to Global.
use super::Budget;
use allocator_api2::alloc::{AllocError, Allocator, Global, Layout};
use std::{ptr::NonNull, sync::Arc};

#[derive(Clone)]
pub(crate) struct BudgetAllocator(pub Arc<Budget>);

// SAFETY: Global supplies layout/alignment and allocation lifetime guarantees.
// All clones share one ledger and may deallocate each other's blocks. Failed
// allocation rolls back its reservation; deallocation releases the bytes once.
unsafe impl Allocator for BudgetAllocator {
    fn allocate(&self, layout: Layout) -> std::result::Result<NonNull<[u8]>, AllocError> {
        self.0.acquire(layout.size()).map_err(|_| AllocError)?;
        match Global.allocate(layout) {
            Ok(block) => Ok(block),
            Err(error) => {
                self.0.release(layout.size());
                Err(error)
            }
        }
    }
    unsafe fn deallocate(&self, pointer: NonNull<u8>, layout: Layout) {
        // SAFETY: Allocator's caller supplies this live Global block and layout.
        unsafe {
            Global.deallocate(pointer, layout);
        }
        self.0.release(layout.size());
    }
}
