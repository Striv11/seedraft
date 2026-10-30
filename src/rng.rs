//! Small seeded PRNG (SplitMix64). No dependencies, fully portable, stable across versions.

#[derive(Debug, Clone)]
pub struct Rng {
    state: u64,
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `lo..=hi`. Panics if `lo > hi`.
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        assert!(lo <= hi, "empty range");
        let span = hi - lo;
        if span == u64::MAX {
            return self.next_u64();
        }
        let n = span + 1;
        // Rejection sampling to avoid modulo bias.
        let zone = u64::MAX - (u64::MAX % n);
        loop {
            let v = self.next_u64();
            if v < zone {
                return lo + v % n;
            }
        }
    }

    /// True with probability `pct`/100 (values >= 100 are always true).
    pub fn chance_pct(&mut self, pct: u8) -> bool {
        self.range(0, 99) < u64::from(pct)
    }

    /// Derive an independent generator, e.g. one per subsystem.
    pub fn fork(&mut self) -> Rng {
        Rng::new(self.next_u64())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vector() {
        // Reference SplitMix64 outputs for seed 0.
        let mut r = Rng::new(0);
        assert_eq!(r.next_u64(), 0xE220_A839_7B1D_CDAF);
        assert_eq!(r.next_u64(), 0x6E78_9E6A_A1B9_65F4);
    }

    #[test]
    fn range_bounds_and_determinism() {
        let mut a = Rng::new(7);
        let mut b = Rng::new(7);
        for _ in 0..1000 {
            let x = a.range(5, 9);
            assert!((5..=9).contains(&x));
            assert_eq!(x, b.range(5, 9));
        }
        assert_eq!(Rng::new(1).range(3, 3), 3);
    }

    #[test]
    fn chance_extremes() {
        let mut r = Rng::new(3);
        assert!((0..200).all(|_| !r.chance_pct(0)));
        assert!((0..200).all(|_| r.chance_pct(100)));
    }

    #[test]
    fn fork_diverges() {
        let mut a = Rng::new(1);
        let mut f = a.fork();
        assert_ne!(a.next_u64(), f.next_u64());
    }
}
