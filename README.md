# soltan-cache-integrity

Integrity testing tool for soltan-cache nodes. Walks the full local cache range and validates data against an agave-rpc reference node.

## What it tests

1. **getBlock integrity** — Every slot in the cache range is queried with all 6 encoding flavors (`json`, `base64`, `jsonParsed`, `json_accounts`, `json_signatures`, `json_none`). Responses from the cache node are compared against the reference node (key-order independent, so `{"a":1,"b":2}` and `{"b":2,"a":1}` are treated as equal).

2. **getTransaction integrity** — Signatures are sampled across the cache range (1 per ~100 slots) and queried with all 3 tx encodings (`json`, `base64`, `jsonParsed`). Cache responses are compared against reference (same key-order independent comparison).

3. **Skipped slot detection** — Identifies slots that the reference node reports as skipped (not confirmed) but the cache node incorrectly returns data for. Tests both `confirmed` and `finalized` commitments. This catches the commitment validation bug where skipped/orphan slots leak through the local tip cache.

## Building

```bash
cargo build --release
```

The binary will be at `target/release/integrity-test`.

## Usage

```bash
# Basic — point at a cache node and a reference node
./integrity-test --cache-host <soltan-cache-host> --ref-host <agave-rpc-host>

# Quieter output (only show failures + summary)
./integrity-test --cache-host <soltan-cache-host> --ref-host <agave-rpc-host> --skip-verbose

# Higher concurrency (default is 50, each check = 2 HTTP requests)
./integrity-test --cache-host <soltan-cache-host> --ref-host <agave-rpc-host> --rps 100

# Custom ports
./integrity-test --cache-host <soltan-cache-host> --ref-host <agave-rpc-host> --cache-port 8899 --ref-port 8899

# No color (for piping to files)
./integrity-test --cache-host <soltan-cache-host> --ref-host <agave-rpc-host> --no-color > results.txt 2>&1
```

The tool auto-discovers the cache range via `getFirstAvailableBlock` and `getSlot`, computes the overlap with the reference node, and runs all three tests sequentially.

## Output

```
=== Soltan-Cache Integrity Test ===
Cache: soltan-cache-node:8899
Ref:   agave-rpc-node:8899
Concurrency: 50
  Cache: 407,853,103 → 407,903,103 (50,000 slots)
  Ref:   407,482,812 → 407,903,103
  Test range: 407,853,203 → 407,903,071 (49,868 slots)

--- getBlock Integrity (49,868 slots x 6 encodings = 299,208 checks) ---
  Progress: 1,000/299,208
  ...
  Result: 299,208/299,208 passed

--- getTransaction Integrity ---
  Testing 498 signatures from 498 slots x 3 encodings = 1,494 checks
  ...
  Result: 1,494/1,494 passed

--- Skipped Slot Detection ---
  Scanning 407,853,203 → 407,903,071 ...
  50 skipped, 49,818 confirmed
  [BUG]  slot 407859584: leaked confirmed+finalized
  ...
  Result: 35/50 clean, 15 leaked (confirmed: 15, finalized: 15)

=== SUMMARY (312.4s) ===
  getBlock:       PASS (299,208/299,208)
  getTransaction: PASS (1,494/1,494)
  Skipped slots:  FAIL (15 leaked of 50 tested)

EXIT CODE: 1
```

Exit code is `0` if all tests pass, `1` if any issues are found.

## Known differences

Soltan-cache serializes some fields differently from agave-rpc:
- **`null` vs `[]`** — For vote transactions with no logs or inner instructions, agave returns `logMessages: null` and `innerInstructions: null`, while soltan-cache returns empty arrays `[]`. This is a serialization difference, not a data integrity issue. These will show up as `data mismatch` in the output.

## How it works

- Uses `reqwest` with HTTP/1.1 connection pooling (one pool per host)
- Concurrency is controlled via `for_each_concurrent` — `--rps 50` means 50 checks running in parallel, each check fires 2 requests (one to cache, one to reference) via `tokio::join!`
- JSON comparison uses recursive deep-sort + equality check, offloaded to `spawn_blocking` to avoid starving the tokio runtime with CPU work on large (1-10 MB) block responses
- Retries transient HTTP errors up to 3 times with backoff
- Trims 100 slots from the oldest edge and 32 from the tip to avoid false positives from cache eviction during the test
