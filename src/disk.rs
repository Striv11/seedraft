//! Simulated per-node disk: an append-only record log with explicit `fsync`.
//!
//! Model: `append` only reaches a volatile buffer. `fsync` makes the buffer durable, unless the
//! simulated drive lies (`fsync_loss_pct`), in which case the data stays volatile. On a crash
//! the volatile bytes are cut at a random point, so the surviving log may end in a *torn*
//! record. Records are framed (`len`, checksum, payload), and reading stops at the first record
//! that is incomplete or fails its checksum, like a real write-ahead log recovery would.

use crate::rng::Rng;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DiskConfig {
    /// Chance (percent) that an `fsync` reports success but persists nothing.
    pub fsync_loss_pct: u8,
    /// Chance (percent) that a crash keeps a random prefix of the unsynced bytes
    /// (otherwise all unsynced bytes vanish).
    pub torn_write_pct: u8,
}

const HEADER: usize = 12; // u32 length + u64 checksum

fn checksum(data: &[u8]) -> u64 {
    // FNV-1a, enough to detect torn payloads.
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in data {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

#[derive(Debug, Clone, Default)]
pub struct Disk {
    durable: Vec<u8>,
    volatile: Vec<u8>,
}

impl Disk {
    pub fn new() -> Self {
        Self::default()
    }

    /// Buffer a record. Not durable until a successful `fsync`.
    pub fn append(&mut self, record: &[u8]) {
        let len = u32::try_from(record.len()).expect("record too large");
        self.volatile.extend_from_slice(&len.to_le_bytes());
        self.volatile.extend_from_slice(&checksum(record).to_le_bytes());
        self.volatile.extend_from_slice(record);
    }

    /// Flush buffered writes. Returns whether data was really persisted (the caller in a
    /// real system could not know; tests and traces can).
    pub fn fsync(&mut self, cfg: &DiskConfig, rng: &mut Rng) -> bool {
        if self.volatile.is_empty() {
            return true;
        }
        if rng.chance_pct(cfg.fsync_loss_pct) {
            return false;
        }
        self.durable.append(&mut self.volatile);
        true
    }

    /// Power loss: unsynced bytes vanish, or (torn write) a random prefix of them survives.
    /// Returns the number of unsynced bytes that survived.
    pub fn crash(&mut self, cfg: &DiskConfig, rng: &mut Rng) -> usize {
        let keep = if !self.volatile.is_empty() && rng.chance_pct(cfg.torn_write_pct) {
            rng.range(0, self.volatile.len() as u64 - 1) as usize
        } else {
            0
        };
        self.durable.extend_from_slice(&self.volatile[..keep]);
        self.volatile.clear();
        keep
    }

    /// Read back all intact records from durable storage, stopping at the first torn one.
    pub fn read_records(&self) -> Vec<Vec<u8>> {
        self.scan().0
    }

    /// Drop a torn tail so later appends start on a record boundary (done by recovery).
    pub fn repair(&mut self) {
        let valid = self.scan().1;
        self.durable.truncate(valid);
    }

    /// Bytes that would survive a crash right now.
    pub fn durable_len(&self) -> usize {
        self.durable.len()
    }

    /// Bytes buffered but not yet durable.
    pub fn unsynced_len(&self) -> usize {
        self.volatile.len()
    }

    fn scan(&self) -> (Vec<Vec<u8>>, usize) {
        let mut out = Vec::new();
        let mut pos = 0;
        let d = &self.durable;
        while d.len() - pos >= HEADER {
            let len = u32::from_le_bytes(d[pos..pos + 4].try_into().unwrap()) as usize;
            let sum = u64::from_le_bytes(d[pos + 4..pos + HEADER].try_into().unwrap());
            let end = pos + HEADER + len;
            if end > d.len() || checksum(&d[pos + HEADER..end]) != sum {
                break;
            }
            out.push(d[pos + HEADER..end].to_vec());
            pos = end;
        }
        (out, pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clean() -> DiskConfig {
        DiskConfig::default()
    }

    #[test]
    fn unsynced_writes_vanish_on_crash() {
        let mut r = Rng::new(1);
        let mut d = Disk::new();
        d.append(b"a");
        assert!(d.fsync(&clean(), &mut r));
        d.append(b"b");
        d.crash(&clean(), &mut r);
        assert_eq!(d.read_records(), vec![b"a".to_vec()]);
    }

    #[test]
    fn lost_fsync_persists_nothing() {
        let cfg = DiskConfig {
            fsync_loss_pct: 100,
            torn_write_pct: 0,
        };
        let mut r = Rng::new(1);
        let mut d = Disk::new();
        d.append(b"a");
        assert!(!d.fsync(&cfg, &mut r));
        d.crash(&cfg, &mut r);
        assert!(d.read_records().is_empty());
    }

    #[test]
    fn torn_write_yields_prefix_of_records() {
        let cfg = DiskConfig {
            fsync_loss_pct: 0,
            torn_write_pct: 100,
        };
        let recs: Vec<Vec<u8>> = (0..6u8).map(|i| vec![i; 5 + i as usize]).collect();
        let mut saw_partial = false;
        for seed in 0..200 {
            let mut r = Rng::new(seed);
            let mut d = Disk::new();
            for rec in &recs {
                d.append(rec);
            }
            d.crash(&cfg, &mut r);
            let got = d.read_records();
            assert!(got.len() < recs.len(), "a torn crash can't keep everything");
            assert_eq!(got[..], recs[..got.len()], "must be a prefix");
            if d.durable_len() > 0 && d.scan().1 < d.durable_len() {
                saw_partial = true; // a torn record tail existed and was ignored
            }
        }
        assert!(saw_partial);
    }

    #[test]
    fn repair_allows_clean_appends_after_torn_tail() {
        let cfg = DiskConfig {
            fsync_loss_pct: 0,
            torn_write_pct: 100,
        };
        let mut r = Rng::new(3);
        let mut d = Disk::new();
        d.append(b"hello world");
        d.append(b"second");
        d.crash(&cfg, &mut r);
        d.repair();
        d.append(b"fresh");
        assert!(d.fsync(&clean(), &mut r));
        assert_eq!(d.read_records().last().unwrap(), b"fresh");
    }

    #[test]
    fn same_seed_same_disk_state() {
        let cfg = DiskConfig {
            fsync_loss_pct: 30,
            torn_write_pct: 60,
        };
        let run = |seed| {
            let mut r = Rng::new(seed);
            let mut d = Disk::new();
            for i in 0..50u8 {
                d.append(&[i; 7]);
                if i % 3 == 0 {
                    d.fsync(&cfg, &mut r);
                }
                if i % 10 == 9 {
                    d.crash(&cfg, &mut r);
                    d.repair();
                }
            }
            d.crash(&cfg, &mut r);
            d.read_records()
        };
        assert_eq!(run(5), run(5));
        assert!((0..20).any(|s| run(s) != run(5)));
    }
}
