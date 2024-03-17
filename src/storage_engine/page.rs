use std::{
    alloc::{alloc_zeroed, Layout},
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

pub struct Page {
    dirty: bool,
    ptr: NonNull<u8>,
}

impl Page {
    pub const SIZE: usize = 4096;
    const LAYOUT: Layout = Layout::new::<[u8; Self::SIZE]>();

    pub fn new() -> Self {
        // SAFETY: we can ensure that `Self::LAYOUT` has a non-zero size
        let ptr = unsafe { alloc_zeroed(Self::LAYOUT) };

        let non_null = NonNull::new(ptr).expect("run out of memory");

        Self {
            ptr: non_null,
            dirty: false,
        }
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    pub fn clear_dirty_sign(&mut self) {
        self.dirty = false;
    }
}

impl Deref for Page {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        // SAFETY:
        // 1. data is valid for reads for 4096 bytes
        // 2. data is non-null and aligned (guaranteed by Layout)
        // 3. all the 4096 bytes are zero-initialized
        // 4. The returned slice won't be modified for the lifetime of 'a
        // 5. 4096 is smaller than isize::MAX
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), Self::SIZE) }
    }
}

impl DerefMut for Page {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.dirty = true;
        // SAFETY:
        // See `deref()`.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), Self::SIZE) }
    }
}
