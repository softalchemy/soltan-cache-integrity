use clap::Parser;
use colored::Colorize;
use futures::stream::{self, StreamExt};
use rand::prelude::IndexedRandom;
use rand::Rng;
use reqwest::Client;
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap};
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// ─────────────────────────────────────────────────────────────────────────────
// CLI
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(about = "Soltan-Cache integrity test: walks the full cache range and validates data against an agave-rpc reference")]
struct Args {
    /// Soltan-cache node hostname
    #[arg(long)]
    cache_host: String,
    /// Agave reference node hostname
    #[arg(long)]
    ref_host: String,
    #[arg(long, default_value_t = 8899)]
    cache_port: u16,
    #[arg(long, default_value_t = 8899)]
    ref_port: u16,
    /// Max concurrent checks (each check = 2 HTTP requests)
    #[arg(long, default_value_t = 50)]
    rps: usize,
    #[arg(long)]
    skip_verbose: bool,
    #[arg(long)]
    no_color: bool,
    /// Randomize non-essential params (encoding, transactionDetails, rewards, etc.) per request
    /// while keeping the block slot / tx signature fixed. Both cache and reference get the same
    /// randomized params so responses are still comparable.
    #[arg(long)]
    random_payload: bool,
}

// ─────────────────────────────────────────────────────────────────────────────
// RPC Client
// ─────────────────────────────────────────────────────────────────────────────

struct RpcClient {
    url: String,
    client: Client,
    id: AtomicU64,
}

impl RpcClient {
    fn new(host: &str, port: u16, client: Client) -> Self {
        Self {
            url: format!("http://{}:{}", host, port),
            client,
            id: AtomicU64::new(0),
        }
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value, String> {
        let id = self.id.fetch_add(1, Ordering::Relaxed);
        let payload = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        let resp = self.client.post(&self.url).json(&payload).send().await.map_err(|e| e.to_string())?;
        let body = resp.bytes().await.map_err(|e| e.to_string())?;
        if body.is_empty() { return Err("empty response body".into()); }
        serde_json::from_slice(&body).map_err(|e| e.to_string())
    }

    async fn call_safe(&self, method: &str, params: Value) -> Value {
        for attempt in 0..3u32 {
            match self.call(method, params.clone()).await {
                Ok(v) => return v,
                Err(e) => {
                    if attempt < 2 {
                        tokio::time::sleep(Duration::from_millis(200 * (attempt as u64 + 1))).await;
                        continue;
                    }
                    return json!({"error": {"code": -1, "message": e}});
                }
            }
        }
        unreachable!()
    }
}

fn has_data(resp: &Value) -> bool {
    resp.get("result").is_some_and(|r| !r.is_null())
}

fn get_error_msg(resp: &Value) -> String {
    resp.get("error").and_then(|e| e.get("message")).and_then(|m| m.as_str()).unwrap_or("null result").to_string()
}

// ─────────────────────────────────────────────────────────────────────────────
// Encoding flavors
// ─────────────────────────────────────────────────────────────────────────────

struct BlockFlavor { name: &'static str, encoding: &'static str, tx_details: &'static str }

const BLOCK_FLAVORS: &[BlockFlavor] = &[
    BlockFlavor { name: "json",            encoding: "json",       tx_details: "full" },
    BlockFlavor { name: "base64",          encoding: "base64",     tx_details: "full" },
    BlockFlavor { name: "jsonParsed",      encoding: "jsonParsed", tx_details: "full" },
    BlockFlavor { name: "json_accounts",   encoding: "json",       tx_details: "accounts" },
    BlockFlavor { name: "json_signatures", encoding: "json",       tx_details: "signatures" },
    BlockFlavor { name: "json_none",       encoding: "json",       tx_details: "none" },
];

const TX_ENCODINGS: &[&str] = &["json", "base64", "jsonParsed"];

// ─────────────────────────────────────────────────────────────────────────────
// Random payload generation
// ─────────────────────────────────────────────────────────────────────────────

const COMMITMENTS: &[&str] = &["confirmed", "finalized"];

// Valid (encoding, transactionDetails) combos that soltan-cache supports.
// base64 and jsonParsed only cache "full"; json caches all txDetails variants.
const BLOCK_COMBOS: &[(&str, &str)] = &[
    ("json", "full"), ("json", "accounts"), ("json", "signatures"), ("json", "none"),
    ("base64", "full"),
    ("jsonParsed", "full"),
];

fn random_block_params(slot: u64) -> (Value, String) {
    let mut rng = rand::rng();
    let &(enc, td) = BLOCK_COMBOS.choose(&mut rng).unwrap();
    let rewards: bool = rng.random();
    let commitment = COMMITMENTS.choose(&mut rng).unwrap();
    let mstv: u8 = if rng.random::<bool>() { 0 } else { rng.random_range(0..=2) };
    let label = format!("{}_{}_r{}_c{}_v{}", enc, td, rewards as u8, &commitment[..3], mstv);
    let params = json!([slot, {
        "encoding": enc, "transactionDetails": td,
        "maxSupportedTransactionVersion": mstv, "rewards": rewards, "commitment": commitment,
    }]);
    (params, label)
}

fn random_tx_params(sig: &str) -> (Value, String) {
    let mut rng = rand::rng();
    let enc = TX_ENCODINGS.choose(&mut rng).unwrap();
    let commitment = COMMITMENTS.choose(&mut rng).unwrap();
    let mstv: u8 = if rng.random::<bool>() { 0 } else { rng.random_range(0..=2) };
    let label = format!("{}_{}_v{}", enc, &commitment[..3], mstv);
    let params = json!([sig, {"encoding": enc, "commitment": commitment, "maxSupportedTransactionVersion": mstv}]);
    (params, label)
}

// ─────────────────────────────────────────────────────────────────────────────
// Semantic JSON comparison (offloaded to blocking thread)
// ─────────────────────────────────────────────────────────────────────────────

fn deep_sort(v: &Value) -> Value {
    match v {
        Value::Object(map) => {
            let sorted: serde_json::Map<String, Value> = map.iter().map(|(k, v)| (k.clone(), deep_sort(v))).collect();
            Value::Object(sorted)
        }
        Value::Array(arr) => {
            let mut items: Vec<Value> = arr.iter().map(deep_sort).collect();
            items.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
            Value::Array(items)
        }
        other => other.clone(),
    }
}

async fn semantic_equal(a: Value, b: Value) -> bool {
    tokio::task::spawn_blocking(move || deep_sort(&a) == deep_sort(&b)).await.unwrap_or(false)
}

// ─────────────────────────────────────────────────────────────────────────────
// Mismatch
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct Mismatch { detail: String }

// ─────────────────────────────────────────────────────────────────────────────
// Range discovery
// ─────────────────────────────────────────────────────────────────────────────

async fn discover_range(cache: &RpcClient, reference: &RpcClient, no_color: bool) -> (u64, u64) {
    let (cs, cf, rs, rf) = tokio::join!(
        cache.call_safe("getSlot", json!([{"commitment": "confirmed"}])),
        cache.call_safe("getFirstAvailableBlock", json!([])),
        reference.call_safe("getSlot", json!([{"commitment": "confirmed"}])),
        reference.call_safe("getFirstAvailableBlock", json!([])),
    );
    let cc = cs["result"].as_u64().unwrap();
    let cfv = cf["result"].as_u64().unwrap();
    let rc = rs["result"].as_u64().unwrap();
    let rfv = rf["result"].as_u64().unwrap();
    let cache_span = cc - cfv;
    // Skip the oldest 20% of the cache window to avoid eviction races
    let safe_start = cfv + cache_span / 5;
    let start = safe_start.max(rfv);
    let end = cc.min(rc) - 32;

    cprint(&format!("  Cache: {} → {} ({} slots)", fmt_num(cfv), fmt_num(cc), fmt_num(cc - cfv)), no_color, None);
    cprint(&format!("  Ref:   {} → {}", fmt_num(rfv), fmt_num(rc)), no_color, None);
    cprint(&format!("  Test range: {} → {} ({} slots)", fmt_num(start), fmt_num(end), fmt_num(end - start)), no_color, None);

    if end <= start {
        cprint("  ERROR: no overlapping range", no_color, Some("red"));
        std::process::exit(1);
    }
    (start, end)
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. getBlock integrity — full range, all encodings
// ─────────────────────────────────────────────────────────────────────────────

async fn test_blocks(
    cache: &RpcClient, reference: &RpcClient,
    start: u64, end: u64, concurrency: usize, verbose: bool, no_color: bool,
    random_payload: bool,
) -> (usize, usize, Vec<Mismatch>) {
    let slots: Vec<u64> = (start..=end).collect();

    if random_payload {
        // Random mode: 1 random param combo per slot
        let total = slots.len();
        cprint(&format!("\n--- getBlock Integrity [RANDOM PAYLOAD] ({} slots x 1 random combo = {} checks) ---", fmt_num(slots.len() as u64), fmt_num(total as u64)), no_color, None);

        let done = Arc::new(AtomicU64::new(0));
        let passed = Arc::new(AtomicU64::new(0));
        let mismatches = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        // Pre-generate random params per slot so both cache & ref get the same
        let tasks: Vec<_> = slots.iter().map(|&s| {
            let (params, label) = random_block_params(s);
            (s, params, label)
        }).collect();

        stream::iter(tasks)
            .for_each_concurrent(concurrency, |(slot, params, label)| {
                let done = done.clone();
                let passed = passed.clone();
                let mismatches = mismatches.clone();
                async move {
                    let (cr, rr) = tokio::join!(
                        cache.call_safe("getBlock", params.clone()),
                        reference.call_safe("getBlock", params),
                    );
                    let d = done.fetch_add(1, Ordering::Relaxed) + 1;
                    let ch = has_data(&cr);
                    let rh = has_data(&rr);

                    if !ch && !rh {
                        passed.fetch_add(1, Ordering::Relaxed);
                    } else if ch && rh {
                        if semantic_equal(cr["result"].clone(), rr["result"].clone()).await {
                            passed.fetch_add(1, Ordering::Relaxed);
                            if verbose { cprint(&format!("  [PASS] slot {} {} ({}/{})", slot, label, d, total), no_color, Some("green")); }
                        } else {
                            cprint(&format!("  [FAIL] slot {} {}: data mismatch ({}/{})", slot, label, d, total), no_color, Some("red"));
                            mismatches.lock().await.push(Mismatch { detail: format!("data mismatch ({})", label) });
                        }
                    } else {
                        let detail = if ch { "cache has data, ref doesn't".into() } else { format!("ref has data, cache: {}", get_error_msg(&cr)) };
                        cprint(&format!("  [FAIL] slot {} {}: {} ({}/{})", slot, label, detail, d, total), no_color, Some("red"));
                        mismatches.lock().await.push(Mismatch { detail });
                    }
                    if d % 1000 == 0 { cprint(&format!("  Progress: {}/{}", fmt_num(d), fmt_num(total as u64)), no_color, None); }
                }
            }).await;

        let p = passed.load(Ordering::Relaxed) as usize;
        let mm = mismatches.lock().await.clone();
        let c = if mm.is_empty() { "green" } else { "red" };
        cprint(&format!("  Result: {}/{} passed", fmt_num(p as u64), fmt_num(total as u64)), no_color, Some(c));
        (p, total, mm)
    } else {
        // Original mode: all encoding flavors per slot
        let total = slots.len() * BLOCK_FLAVORS.len();
        cprint(&format!("\n--- getBlock Integrity ({} slots x {} encodings = {} checks) ---", fmt_num(slots.len() as u64), BLOCK_FLAVORS.len(), fmt_num(total as u64)), no_color, None);

        let done = Arc::new(AtomicU64::new(0));
        let passed = Arc::new(AtomicU64::new(0));
        let mismatches = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        let tasks: Vec<_> = slots.iter().flat_map(|&s| BLOCK_FLAVORS.iter().map(move |f| (s, f))).collect();

        stream::iter(tasks)
            .for_each_concurrent(concurrency, |(slot, flavor)| {
                let done = done.clone();
                let passed = passed.clone();
                let mismatches = mismatches.clone();
                async move {
                    let params = json!([slot, {
                        "encoding": flavor.encoding, "transactionDetails": flavor.tx_details,
                        "maxSupportedTransactionVersion": 0, "rewards": false, "commitment": "confirmed",
                    }]);
                    let (cr, rr) = tokio::join!(
                        cache.call_safe("getBlock", params.clone()),
                        reference.call_safe("getBlock", params),
                    );
                    let d = done.fetch_add(1, Ordering::Relaxed) + 1;
                    let ch = has_data(&cr);
                    let rh = has_data(&rr);

                    if !ch && !rh {
                        passed.fetch_add(1, Ordering::Relaxed);
                    } else if ch && rh {
                        if semantic_equal(cr["result"].clone(), rr["result"].clone()).await {
                            passed.fetch_add(1, Ordering::Relaxed);
                            if verbose { cprint(&format!("  [PASS] slot {} {} ({}/{})", slot, flavor.name, d, total), no_color, Some("green")); }
                        } else {
                            cprint(&format!("  [FAIL] slot {} {}: data mismatch ({}/{})", slot, flavor.name, d, total), no_color, Some("red"));
                            mismatches.lock().await.push(Mismatch { detail: "data mismatch".into() });
                        }
                    } else {
                        let detail = if ch { "cache has data, ref doesn't".into() } else { format!("ref has data, cache: {}", get_error_msg(&cr)) };
                        cprint(&format!("  [FAIL] slot {} {}: {} ({}/{})", slot, flavor.name, detail, d, total), no_color, Some("red"));
                        mismatches.lock().await.push(Mismatch { detail });
                    }
                    if d % 1000 == 0 { cprint(&format!("  Progress: {}/{}", fmt_num(d), fmt_num(total as u64)), no_color, None); }
                }
            }).await;

        let p = passed.load(Ordering::Relaxed) as usize;
        let mm = mismatches.lock().await.clone();
        let c = if mm.is_empty() { "green" } else { "red" };
        cprint(&format!("  Result: {}/{} passed", fmt_num(p as u64), fmt_num(total as u64)), no_color, Some(c));
        (p, total, mm)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. getTransaction integrity — sample 1 tx per evenly-spaced slot
// ─────────────────────────────────────────────────────────────────────────────

async fn test_transactions(
    cache: &RpcClient, reference: &RpcClient,
    start: u64, end: u64, concurrency: usize, verbose: bool, no_color: bool,
    random_payload: bool,
) -> (usize, usize, Vec<Mismatch>) {
    let mode_label = if random_payload { " [RANDOM PAYLOAD]" } else { "" };
    cprint(&format!("\n--- getTransaction Integrity{} ---", mode_label), no_color, None);

    // Pick 1 sig from every ~100th slot across the range
    let span = end - start;
    let num_sigs = (span / 100).max(10).min(500) as usize;
    let step = (span / num_sigs as u64).max(1);
    let candidate_slots: Vec<u64> = (start..=end).step_by(step as usize).take(num_sigs).collect();

    cprint(&format!("  Extracting ~{} signatures from range...", candidate_slots.len()), no_color, None);

    let collected = Arc::new(tokio::sync::Mutex::new(Vec::<(String, u64)>::new()));
    stream::iter(candidate_slots)
        .for_each_concurrent(concurrency, |slot| {
            let collected = collected.clone();
            async move {
                let r = reference.call_safe("getBlock", json!([slot, {
                    "encoding": "json", "transactionDetails": "signatures",
                    "rewards": false, "maxSupportedTransactionVersion": 0, "commitment": "confirmed",
                }])).await;
                if let Some(sigs) = r.get("result").and_then(|r| r.get("signatures")).and_then(|s| s.as_array()) {
                    if let Some(sig) = sigs.first().and_then(|s| s.as_str()) {
                        collected.lock().await.push((sig.to_string(), slot));
                    }
                }
            }
        }).await;

    let sig_pairs = Arc::try_unwrap(collected).unwrap().into_inner();
    if sig_pairs.is_empty() {
        cprint("  WARNING: no signatures found", no_color, Some("yellow"));
        return (0, 0, vec![]);
    }

    if random_payload {
        let total = sig_pairs.len();
        let unique: BTreeSet<u64> = sig_pairs.iter().map(|(_, s)| *s).collect();
        cprint(&format!("  Testing {} signatures from {} slots x 1 random combo = {} checks", sig_pairs.len(), unique.len(), total), no_color, None);

        let done = Arc::new(AtomicU64::new(0));
        let passed = Arc::new(AtomicU64::new(0));
        let mismatches = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        // Pre-generate random params per sig
        let tasks: Vec<_> = sig_pairs.iter().map(|(sig, slot)| {
            let (params, label) = random_tx_params(sig);
            (sig.clone(), *slot, params, label)
        }).collect();

        stream::iter(tasks)
            .for_each_concurrent(concurrency, |(sig, slot, params, label)| {
                let done = done.clone();
                let passed = passed.clone();
                let mismatches = mismatches.clone();
                async move {
                    let (cr, rr) = tokio::join!(
                        cache.call_safe("getTransaction", params.clone()),
                        reference.call_safe("getTransaction", params),
                    );
                    let d = done.fetch_add(1, Ordering::Relaxed) + 1;
                    let ch = has_data(&cr);
                    let rh = has_data(&rr);
                    let ss = &sig[..16.min(sig.len())];

                    if !ch && !rh {
                        passed.fetch_add(1, Ordering::Relaxed);
                    } else if ch && rh {
                        if semantic_equal(cr["result"].clone(), rr["result"].clone()).await {
                            passed.fetch_add(1, Ordering::Relaxed);
                            if verbose { cprint(&format!("  [PASS] tx {}... ({}) slot {} ({}/{})", ss, label, slot, d, total), no_color, Some("green")); }
                        } else {
                            cprint(&format!("  [FAIL] tx {}... ({}) slot {}: data mismatch ({}/{})", ss, label, slot, d, total), no_color, Some("red"));
                            mismatches.lock().await.push(Mismatch { detail: format!("data mismatch ({})", label) });
                        }
                    } else {
                        let detail = if ch { "cache has data, ref doesn't".into() } else { format!("ref has data, cache: {}", get_error_msg(&cr)) };
                        cprint(&format!("  [FAIL] tx {}... ({}) slot {}: {} ({}/{})", ss, label, slot, detail, d, total), no_color, Some("red"));
                        mismatches.lock().await.push(Mismatch { detail });
                    }
                    if d % 200 == 0 { cprint(&format!("  Progress: {}/{}", d, total), no_color, None); }
                }
            }).await;

        let p = passed.load(Ordering::Relaxed) as usize;
        let mm = mismatches.lock().await.clone();
        let c = if mm.is_empty() { "green" } else { "red" };
        cprint(&format!("  Result: {}/{} passed", p, total), no_color, Some(c));
        (p, total, mm)
    } else {
        let total = sig_pairs.len() * TX_ENCODINGS.len();
        let unique: BTreeSet<u64> = sig_pairs.iter().map(|(_, s)| *s).collect();
        cprint(&format!("  Testing {} signatures from {} slots x {} encodings = {} checks", sig_pairs.len(), unique.len(), TX_ENCODINGS.len(), total), no_color, None);

        let done = Arc::new(AtomicU64::new(0));
        let passed = Arc::new(AtomicU64::new(0));
        let mismatches = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        let tasks: Vec<_> = sig_pairs.iter().flat_map(|(sig, slot)| TX_ENCODINGS.iter().map(move |enc| (sig.clone(), *slot, *enc))).collect();

        stream::iter(tasks)
            .for_each_concurrent(concurrency, |(sig, slot, enc)| {
                let done = done.clone();
                let passed = passed.clone();
                let mismatches = mismatches.clone();
                async move {
                    let params = json!([sig, {"encoding": enc, "commitment": "confirmed", "maxSupportedTransactionVersion": 0}]);
                    let (cr, rr) = tokio::join!(
                        cache.call_safe("getTransaction", params.clone()),
                        reference.call_safe("getTransaction", params),
                    );
                    let d = done.fetch_add(1, Ordering::Relaxed) + 1;
                    let ch = has_data(&cr);
                    let rh = has_data(&rr);
                    let ss = &sig[..16.min(sig.len())];

                    if !ch && !rh {
                        passed.fetch_add(1, Ordering::Relaxed);
                    } else if ch && rh {
                        if semantic_equal(cr["result"].clone(), rr["result"].clone()).await {
                            passed.fetch_add(1, Ordering::Relaxed);
                            if verbose { cprint(&format!("  [PASS] tx {}... ({}) slot {} ({}/{})", ss, enc, slot, d, total), no_color, Some("green")); }
                        } else {
                            cprint(&format!("  [FAIL] tx {}... ({}) slot {}: data mismatch ({}/{})", ss, enc, slot, d, total), no_color, Some("red"));
                            mismatches.lock().await.push(Mismatch { detail: "data mismatch".into() });
                        }
                    } else {
                        let detail = if ch { "cache has data, ref doesn't".into() } else { format!("ref has data, cache: {}", get_error_msg(&cr)) };
                        cprint(&format!("  [FAIL] tx {}... ({}) slot {}: {} ({}/{})", ss, enc, slot, detail, d, total), no_color, Some("red"));
                        mismatches.lock().await.push(Mismatch { detail });
                    }
                    if d % 200 == 0 { cprint(&format!("  Progress: {}/{}", d, total), no_color, None); }
                }
            }).await;

        let p = passed.load(Ordering::Relaxed) as usize;
        let mm = mismatches.lock().await.clone();
        let c = if mm.is_empty() { "green" } else { "red" };
        cprint(&format!("  Result: {}/{} passed", p, total), no_color, Some(c));
        (p, total, mm)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. Skipped slot leak detection — full range
// ─────────────────────────────────────────────────────────────────────────────

async fn test_skipped(
    cache: &RpcClient, reference: &RpcClient,
    start: u64, end: u64, concurrency: usize, verbose: bool, no_color: bool,
) -> (usize, usize, Vec<u64>, Vec<u64>) {
    cprint("\n--- Skipped Slot Detection ---", no_color, None);

    let mut total_gaps = 0usize;
    let mut total_clean = 0usize;
    let mut leaked_confirmed: Vec<u64> = Vec::new();
    let mut leaked_finalized: Vec<u64> = Vec::new();

    let mut s = start;
    while s < end {
        let e = (s + 49_999).min(end);
        cprint(&format!("  Scanning {} → {} ...", fmt_num(s), fmt_num(e)), no_color, None);

        let br = reference.call_safe("getBlocks", json!([s, e, {"commitment": "confirmed"}])).await;
        let confirmed: BTreeSet<u64> = br["result"].as_array().unwrap_or(&vec![]).iter().filter_map(|v| v.as_u64()).collect();
        let gaps: Vec<u64> = (s..=e).filter(|x| !confirmed.contains(x)).collect();

        cprint(&format!("  {} skipped, {} confirmed", gaps.len(), confirmed.len()), no_color, None);
        total_gaps += gaps.len();

        if !gaps.is_empty() {
            // Probe cache for all gap slots
            let results = Arc::new(tokio::sync::Mutex::new(Vec::new()));
            stream::iter(gaps.iter().copied())
                .for_each_concurrent(concurrency, |slot| {
                    let results = results.clone();
                    async move {
                        let r = cache.call_safe("getBlock", json!([slot, {"commitment": "confirmed", "transactionDetails": "none", "rewards": false, "maxSupportedTransactionVersion": 0}])).await;
                        results.lock().await.push((slot, has_data(&r)));
                    }
                }).await;

            let probes = results.lock().await;
            let mut hits = Vec::new();
            for &(slot, has) in probes.iter() {
                if has { hits.push(slot); } else {
                    total_clean += 1;
                    if verbose { cprint(&format!("  [OK]   slot {}: clean", slot), no_color, Some("green")); }
                }
            }
            drop(probes);

            if !hits.is_empty() {
                // Double-check on reference
                let ref_results = Arc::new(tokio::sync::Mutex::new(Vec::new()));
                stream::iter(hits.iter().copied())
                    .for_each_concurrent(concurrency, |slot| {
                        let results = ref_results.clone();
                        async move {
                            let r = reference.call_safe("getBlock", json!([slot, {"commitment": "confirmed", "transactionDetails": "none", "rewards": false, "maxSupportedTransactionVersion": 0}])).await;
                            results.lock().await.push((slot, has_data(&r)));
                        }
                    }).await;

                let rc = ref_results.lock().await;
                let mut leaks = Vec::new();
                for &(slot, has) in rc.iter() {
                    if has { total_clean += 1; if verbose { cprint(&format!("  [OK]   slot {}: false positive", slot), no_color, Some("green")); } }
                    else { leaks.push(slot); leaked_confirmed.push(slot); }
                }
                drop(rc);

                if !leaks.is_empty() {
                    // Test finalized too
                    let fin_results = Arc::new(tokio::sync::Mutex::new(Vec::new()));
                    stream::iter(leaks.iter().copied())
                        .for_each_concurrent(concurrency, |slot| {
                            let results = fin_results.clone();
                            async move {
                                let r = cache.call_safe("getBlock", json!([slot, {"commitment": "finalized", "transactionDetails": "none", "rewards": false, "maxSupportedTransactionVersion": 0}])).await;
                                results.lock().await.push((slot, has_data(&r)));
                            }
                        }).await;

                    for &(slot, has) in fin_results.lock().await.iter() {
                        if has {
                            leaked_finalized.push(slot);
                            cprint(&format!("  [BUG]  slot {}: leaked confirmed+finalized", slot), no_color, Some("red"));
                        } else {
                            cprint(&format!("  [BUG]  slot {}: leaked confirmed only", slot), no_color, Some("red"));
                        }
                    }
                }
            }
        }
        s = e + 1;
    }

    let lc = leaked_confirmed.len();
    let lf = leaked_finalized.len();
    let c = if lc == 0 { "green" } else { "red" };
    cprint(&format!("  Result: {}/{} clean, {} leaked (confirmed: {}, finalized: {})", total_clean, total_gaps, lc, lc, lf), no_color, Some(c));
    (total_clean, total_gaps, leaked_confirmed, leaked_finalized)
}

// ─────────────────────────────────────────────────────────────────────────────
// Output helpers
// ─────────────────────────────────────────────────────────────────────────────

fn cprint(msg: &str, no_color: bool, color: Option<&str>) {
    if no_color || color.is_none() { println!("{}", msg); }
    else { match color.unwrap() {
        "red" => println!("{}", msg.red()),
        "green" => println!("{}", msg.green()),
        "yellow" => println!("{}", msg.yellow()),
        _ => println!("{}", msg),
    }}
}

fn fmt_num(n: u64) -> String {
    let s = n.to_string();
    let mut r = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 { r.push(','); }
        r.push(c);
    }
    r.chars().rev().collect()
}

fn breakdown(mm: &[Mismatch]) -> HashMap<String, usize> {
    let mut c = HashMap::new();
    for m in mm { *c.entry(m.detail.clone()).or_insert(0) += 1; }
    c
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    let verbose = !args.skip_verbose;
    let no_color = args.no_color;
    let concurrency = args.rps;

    cprint("=== Soltan-Cache Integrity Test ===", no_color, None);
    cprint(&format!("Cache: {}:{}", args.cache_host, args.cache_port), no_color, None);
    cprint(&format!("Ref:   {}:{}", args.ref_host, args.ref_port), no_color, None);
    cprint(&format!("Concurrency: {}", concurrency), no_color, None);
    if args.random_payload { cprint("Mode: RANDOM PAYLOAD (randomized encoding/txDetails/rewards/commitment/mstv per request)", no_color, Some("yellow")); }

    let build_client = || Client::builder()
        .pool_max_idle_per_host(concurrency)
        .pool_idle_timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(120))
        .tcp_keepalive(Duration::from_secs(30))
        .http1_only()
        .build().unwrap();

    let cache = RpcClient::new(&args.cache_host, args.cache_port, build_client());
    let reference = RpcClient::new(&args.ref_host, args.ref_port, build_client());

    let (start, end) = discover_range(&cache, &reference, no_color).await;
    let timer = Instant::now();
    let mut exit_code = 0;

    // 1. getBlock — full range, all encodings
    let (bp, bt, bmm) = test_blocks(&cache, &reference, start, end, concurrency, verbose, no_color, args.random_payload).await;
    if !bmm.is_empty() { exit_code = 1; }

    // 2. getTransaction — sampled across full range
    let (tp, tt, tmm) = test_transactions(&cache, &reference, start, end, concurrency, verbose, no_color, args.random_payload).await;
    if !tmm.is_empty() { exit_code = 1; }

    // 3. Skipped slot detection — full range
    let (_, total_gaps, leaked_confirmed, leaked_finalized) = test_skipped(&cache, &reference, start, end, concurrency, verbose, no_color).await;
    if !leaked_confirmed.is_empty() { exit_code = 1; }

    // Summary
    let elapsed = timer.elapsed();
    cprint(&format!("\n=== SUMMARY ({:.1}s) ===", elapsed.as_secs_f64()), no_color, None);

    let bc = if bp == bt { "green" } else { "red" };
    cprint(&format!("  getBlock:       {} ({}/{})", if bp == bt { "PASS" } else { "FAIL" }, fmt_num(bp as u64), fmt_num(bt as u64)), no_color, Some(bc));
    if !bmm.is_empty() { for (r, c) in breakdown(&bmm) { cprint(&format!("    {}: {}", r, c), no_color, Some("red")); } }

    let tc = if tp == tt { "green" } else { "red" };
    cprint(&format!("  getTransaction: {} ({}/{})", if tp == tt { "PASS" } else { "FAIL" }, tp, tt), no_color, Some(tc));
    if !tmm.is_empty() { for (r, c) in breakdown(&tmm) { cprint(&format!("    {}: {}", r, c), no_color, Some("red")); } }

    let sc = if leaked_confirmed.is_empty() { "green" } else { "red" };
    cprint(&format!("  Skipped slots:  {} ({} leaked of {} tested)", if leaked_confirmed.is_empty() { "PASS" } else { "FAIL" }, leaked_confirmed.len(), total_gaps), no_color, Some(sc));
    if !leaked_confirmed.is_empty() { cprint(&format!("    Leaked (confirmed): {:?}", leaked_confirmed), no_color, Some("red")); }
    if !leaked_finalized.is_empty() { cprint(&format!("    Leaked (finalized): {:?}", leaked_finalized), no_color, Some("red")); }

    let ec = if exit_code == 0 { "green" } else { "red" };
    cprint(&format!("\nEXIT CODE: {}", exit_code), no_color, Some(ec));
    if exit_code == 0 { ExitCode::SUCCESS } else { ExitCode::FAILURE }
}
