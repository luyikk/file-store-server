use std::hash::{BuildHasher, Hasher};

/// u64 skip hash calculation
/// other panic
#[derive(Default)]
pub(crate) struct NonHasher {
    hash: u64,
}

impl Hasher for NonHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.hash
    }
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        panic!("non u64 type cannot be used NullHasher:{:?}", bytes);
    }
    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.hash = i;
    }
}

/// Non hasher builder
/// only u64 can be used
pub(crate) struct NonHasherBuilder;

impl BuildHasher for NonHasherBuilder {
    type Hasher = NonHasher;

    #[inline]
    fn build_hasher(&self) -> Self::Hasher {
        NonHasher::default()
    }
}
