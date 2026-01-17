# BRIN-like Virtual Index for SQLite (In-Memory Prototype)

This project implements a **BRIN-inspired (Block Range INdex)** access method for SQLite using a **virtual table extension**.  
The index is **built in memory**, targets **naturally ordered data** (logs, timestamps, monotonically increasing values), and is designed for **large append-only tables**.

> ⚠️ This is a prototype and does **not** replace SQLite’s native B-tree indexes.  
> It provides an alternative access path that can outperform B-trees for specific workloads.

---

## 1. Motivation

SQLite’s default B-tree indexes are general-purpose and excellent for random-access workloads.  
However, for **append-only tables with naturally ordered values**, such as:

- logs
- timestamps
- event IDs
- monotonically increasing counters

a B-tree index can become **overkill**:

- high maintenance cost on insert
- deeper trees as data grows
- random I/O patterns

BRIN indexes trade **precision for speed** by indexing **ranges of rows**, not individual values.

---

## 2. What This Extension Does

This extension creates a **virtual table** that stores **metadata ranges** over a base table:

Each BRIN block stores:

| Field          | Meaning |
|---------------|--------|
| `min`         | Minimum value in the block |
| `max`         | Maximum value in the block |
| `start_rowid` | First rowid in the block |
| `end_rowid`   | Last rowid in the block |

Blocks are built by scanning the base table **in rowid order**, grouping rows in fixed-size blocks.

Example (block_size = 1000):



---

## 3. Key Assumptions (Very Important)

This prototype assumes:

- Data is **inserted in increasing order**
- Indexed column is **monotonic** (timestamps, logs)
- `rowid` increases with inserts
- No overlapping ranges

If these assumptions do **not** hold, correctness and performance degrade.

---

## 4. How the Index Is Built

The index is built **lazily**:

- The first query that accesses the virtual table triggers `brinBuildIndex()`
- The entire base table is scanned **once**
- All block ranges are stored in memory
- Subsequent queries reuse the in-memory index

The index **lives only for the SQLite session**.

---

## 5. How to Use the Index

### Step 1: Load the extension

```sql
.load ./brin
CREATE TABLE logs ( ts INTEGER, message TEXT );
CREATE VIRTUAL TABLE brin_idx USING brin(logs, ts, 1024);
```
### Step 2: Use the BRIN metadata table to restrict rowid ranges.
```sql
SELECT l.*
FROM logs AS l
JOIN brin_idx AS b
  ON l.rowid BETWEEN b.start_rowid AND b.end_rowid
WHERE b.min <= 200
  AND b.max >= 100
  AND l.ts BETWEEN 100 AND 200;
```
What happens internally:
1. BRIN metadata blocks are scanned (very small)
2. Matching rowid ranges are identified
3. Only relevant portions of the base table are read

---

## 6. Why This Is Faster (Cost Explanation)

Without BRIN
```sql
SELECT * FROM logs WHERE ts BETWEEN 100 AND 200;
```
Cost (no index):
- O(N) full table scan
Cost (B-tree index):
- O(log N) index traversal
- random I/O
- index maintenance on insert

---

With BRIN-style access

Let:
- N = total rows
- B = block size
- K = N / B = number of BRIN blocks

Costs:
1. Scan BRIN metadata → O(K)
2. Select matching blocks → usually very small
3. Scan selected rowid ranges → O(B × matched_blocks)

Total
```css
O(N / B + B)
```
For large N and reasonable B:
```css
N / B << log N
```

---

## 7. Asymptotic Advantage for Ordered Data

Because values are naturally ordered:
- min is the first value
- max is the last value
- No need to compute aggregates
- No overlapping ranges

This allows:
- early block elimination
- cache-friendly access
- sequential I/O patterns

The result is a lower constant factor and better asymptotic behavior for range queries on ordered data.

---

## 8. Important Limitations

- ❌ Not integrated into SQLite’s planner
- ❌ Requires explicit JOIN
- ❌ In-memory only
- ❌ No incremental maintenance yet
- ❌ No support for random inserts


---

## 9. When This Makes Sense

- ✔ Large append-only tables
- ✔ Time-series or logs
- ✔ Range queries
- ✔ Analytical workloads
- ✔ Low insert cost requirement
