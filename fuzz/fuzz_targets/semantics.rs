#![no_main]

use std::collections::{HashMap, HashSet};

use libfuzzer_sys::fuzz_target;
use mk_core::testing::{self, FuzzClassified, FuzzModifier};

// Receive-path classification: kind/tags/content plus the drain-time
// context (grant, expires_at, now). Modifiers are resolved against a
// fuzz-provided index of known `(id -> (author, kind))` targets.
//
// Layout: `[kind u64][now u64][expires u64][grant u8][final u8][sender 32B]`
// then `n` 72-byte `(id 32B | author 32B | kind u64)` known-target entries,
// then UTF-8: line 1 = content, remaining lines = tags with '\t' fields.
fn u64_at(data: &[u8], off: usize) -> u64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&data[off..off + 8]);
    u64::from_le_bytes(bytes)
}

fuzz_target!(|data: &[u8]| {
    const HEADER: usize = 8 * 3 + 2 + 32 + 1;
    if data.len() < HEADER {
        return;
    }
    let kind = u64_at(data, 0);
    let now_secs = u64_at(data, 8);
    let expires_raw = u64_at(data, 16);
    let grant = match data[24] % 3 {
        0 => None,
        1 => Some(false),
        _ => Some(true),
    };
    let final_attempt = data[25] & 1 == 1;
    let mut sender = [0u8; 32];
    sender.copy_from_slice(&data[26..58]);
    let known_count = usize::from(data[58]);
    let mut off = HEADER;
    let mut known: HashMap<[u8; 32], ([u8; 32], u64)> = HashMap::new();
    for _ in 0..known_count {
        if data.len() < off + 72 {
            return;
        }
        let mut id = [0u8; 32];
        id.copy_from_slice(&data[off..off + 32]);
        let mut author = [0u8; 32];
        author.copy_from_slice(&data[off + 32..off + 64]);
        known.insert(id, (author, u64_at(data, off + 64)));
        off += 72;
    }
    let Ok(text) = std::str::from_utf8(&data[off..]) else {
        return;
    };
    let mut lines = text.lines();
    let content = lines.next().unwrap_or_default();
    let tags: Vec<Vec<String>> = lines
        .map(|line| line.split('\t').map(str::to_owned).collect())
        .collect();
    let expires_at = (expires_raw != u64::MAX).then_some(expires_raw);

    match testing::classify(kind, &tags, content, grant, expires_at, now_secs) {
        FuzzClassified::Reaction { emoji } => {
            assert!(!emoji.is_empty());
            assert_eq!(emoji.trim(), emoji);
            assert!(emoji.chars().count() <= 64);
        }
        FuzzClassified::Modifier(modifier) => {
            if let FuzzModifier::Delete { targets } = &modifier {
                assert!((1..=100).contains(&targets.len()));
                let distinct: HashSet<_> = targets.iter().collect();
                assert_eq!(distinct.len(), targets.len());
            }
            let _ = testing::resolve_modifier(&modifier, sender, &known, final_attempt);
        }
        FuzzClassified::Deliver | FuzzClassified::Drop => {}
    }
});
