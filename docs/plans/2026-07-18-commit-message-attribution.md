# Plan: Commit-message attribution (simplified)

**Date**: 2026-07-18
**Status**: Implemented
**Task**: A secret-scanner consumer joins findings to commits via
`HunkAddition.Commit()` / `ScanMeta.Commit` and calls `GetCommitMetadata` for
attribution. That call must also return the commit message — correctly and
with as little added cost as possible. Experimental library; no API-contract
ceremony.

## Chosen Design

**Rung 1**: extend the existing attribution path in place. No new exported
method, no new option, no new cache.

```go
type CommitMetadata struct {
    Author    AuthorInfo
    Timestamp int64
    Message   string // raw message: bytes after the first blank line
}
```

Mechanics (small diff from what exists):

1. `store.readCommitPayload(oid)` replaces `readCommitHeader` **only inside
   metaCache** (the commit walk keeps `readCommitHeader` untouched — zero
   risk to scan paths). Same structure as readCommitHeader (store.go:710):
   - Plain packed commit: stream the zlib to EOF instead of stopping at the
     `committer ` line. Adversarially-verified cost: commits are tiny (this
     repo's pack: mean 398 B compressed, max 1.2 KiB; 199/200 stored
     non-delta) and the existing 8 KiB bufio first-fill already inflates the
     whole typical object — the early exit was saving almost nothing. No
     cache writes, exactly like today. Defensive size cap (single constant,
     e.g. 64 MiB) against adversarial giant commits; this also *removes* the
     existing MaxHdr=4096 failure mode (huge author lines / octopus parent
     lists currently break `GetCommitMetadata`; the payload path has no
     header cap).
   - Delta or loose commit (~0.5% of commits): `getMaterialized` + copy the
     whole payload — identical to today's fallback minus the trim. The copy
     is mandatory: materialized bytes may alias shared cache buffers, and
     cached strings must not pin them.
2. Split at the first `\n\n`: `header, message = payload[:i], payload[i+2:]`
   (no separator → message empty). **Split before parsing** — a commit
   *message* can legally contain a line starting with `author ` at column 0,
   which would corrupt `parseAuthorHeader` if fed the full payload.
   `parseAuthorHeader` itself is unchanged: within the header half, gpgsig /
   mergetag continuation lines are space-prefixed, so no embedded line can
   false-match `author ` / `committer ` at column 0. The `\n\n` rule was
   byte-verified against all 664 commits of the betterleaks repo plus crafted
   gpgsig / mergetag / encoding / NUL / empty-message / no-separator commits.
   Raw bytes, no normalization (git-log's `%B` re-encoding and NUL truncation
   are presentation behaviors, not object format).
3. metaCache entry becomes `{AuthorInfo, Message string}`; same unbounded
   insert-only map, same RWMutex protocol, same double-parse-on-concurrent-
   miss tolerance. The message is a fresh copy (never `btostr` over the
   payload — that would pin the buffer, and on materialized paths dangle).
   Memory: typical messages are ~200-700 B, so 10k commits ≈ a few MB;
   the existing cache already retains comparable header bytes unbounded.
   Documented in the cache's doc comment, not engineered around.

**Rejected rungs**:
- Rung 0 (no change): no exported API can reach the message; the header read
  stops at the `committer ` line by design (store.go:776).
- Rung 2, separate `GetCommitMessage` + budgeted cache (the previous draft):
  refuted by adversarial review. The "don't burden GetCommitMetadata with
  full inflation" premise doesn't hold — the streaming read already inflates
  typical commits fully, and no internal path calls GetCommitMetadata, so
  there is nothing to regress. Two methods would need write-through cache
  coupling just to avoid double reads; the pair_cache-style budget machinery
  has no named finding at experimental scale to justify it.
- Rung 3+ (new module/abstraction/dependency): nothing requires them.

**Kill-shot for the chosen design**: monorepo-scale memory. At ~1.3M commits
(kernel-scale), unbounded cached messages add roughly 0.3-1 GB on top of the
existing cache's comparable debt, with a heavy tail from multi-KB merge
shortlog messages. If that ever bites, the recorded escape hatches are, in
order: don't-retain-oversize-messages flag (one bool per entry), or the
pair_cache budget pattern. Neither is built now.

## Assumptions

- Consumers call `GetCommitMetadata` from concurrent scan workers
  (`DiffHistoryHunksFunc` invokes callbacks from up to NumCPU goroutines) —
  metaCache's existing locking already serves exactly this shape; verified.
- Raw message bytes are the right return value; any normalization (e.g.
  matching a git-log text parser's title/body joining) belongs to the
  consumer. Open bet — cheap to change later, it's one exported field.
- SHA-1-only `Hash` is inherited pre-existing scope, orthogonal to this
  change.
- `Timestamp` semantics unchanged (committer time from graph when available,
  author-time fallback); `git.date`-style consumers wanting author time use
  `Author.When`.

## Open Unknowns

- Defensive payload size-cap value (64 MiB placeholder) — settle during
  implementation; anything ≥ the largest real-world commit message with
  margin is fine.
  **Resolved**: kept 64 MiB (`maxCommitPayload`, store.go). The implementation
  enforces it *before* the payload allocation on the packed path by parsing
  the declared size from the pack object header, so an adversarial header
  cannot force a giant allocation.
- Whether the interrupted correctness review's remaining angle (mock/test
  churn from the `commitHeaderReader` interface rename) hides anything —
  resolve in step 3 when updating `commit_attribution_test.go` mocks; the
  interface has exactly one method and two mock implementations.
  **Resolved**: nothing hidden. The rename to `commitPayloadReader` touched
  the two mocks plus one misplaced test in `diff_blob_test.go`
  (`TestMetaCacheConcurrency`) that pre-populates the cache map directly.

## Implementation Notes (deviations from the mechanics above)

- Step 1 used the pack header's declared decompressed size + the existing
  `inflateExact` one-shot path instead of extending the streaming
  read-to-EOF loop: one exact-size allocation, no pooled-buffer growth, no
  final copy, and the libdeflate backend's one-shot decode for free. Same
  bytes (differential-tested), better numbers.
- Measured (arm64, very-large-repo-1k fixture, klauspost fork
  `pr4-flate-bytes-reader-dispatch`, benchstat n=10):
  - `readCommitPayload` vs old `readCommitHeader`: 4.15µs vs 4.51µs/op
    (−8%), 255 B vs 434 B/op, 1 vs 4 allocs/op — full inflation is
    *cheaper* than the early-exit line scan, confirming the Rung-1 premise.
  - `GetCommitMetadata` cold: −5.7% time, −30% B/op, −50% allocs/op.
    Warm (cache hit): +5ns (~96ns/op, 0 allocs) from the larger map value.
  - End-to-end `DiffHistoryHunksFunc` + per-hunk `GetCommitMetadata` over
    this repo's own history: wall time within noise (p≥0.22, n=6), peak
    RSS steady, identical hunk count.
- Bonus pinned by test: the payload path removes the `MaxHdr`=4096 failure
  mode — a 100-parent octopus commit fails `readCommitHeader` but
  attributes correctly via `readCommitPayload`
  (`TestCommitPayload_HeaderLargerThanMaxHdr`).

## Implementation Steps

1. `store.readCommitPayload` (rename + extend `readCommitHeaderFromStream`
   path to read-to-EOF; delta/loose fallback returns untrimmed copy) — verify
   by test: payload of a plain packed, a delta-chained, and a loose commit
   each byte-equal `git cat-file commit` output (repo built in `t.TempDir`
   behind `exec.LookPath("git")` skip guard — the store_differential
   pattern).
2. Payload split helper + `Message` on `CommitMetadata`, wired through
   `metaCache.get` with the split-before-parse order — verify by table test:
   plain, multiline, gpgsig-signed, mergetag, `encoding` header (raw bytes
   kept), embedded NUL (kept), empty message, header-only/no-separator, and
   the hazard case: message containing an `author ...` line at column 0 must
   not alter parsed author.
3. Update `commitHeaderReader` interface name/semantics + test mocks;
   existing attribution tests keep passing with `Message` additionally
   asserted — verify by `go test ./... -race`.
4. Integration: for every commit in the generated fixtures (simple-linear,
   with-merges, large-repo), `Message` equals bytes-after-first-`\n\n` of
   `git cat-file commit` (byte-faithful oracle — **not** `git log
   --format=%B`, which re-encodes and NUL-truncates); author fields
   non-empty; plus concurrent duplicate-OID calls under `-race`.
5. Sanity on this repo's own history: `DiffHistoryHunksFunc` scan calling
   `GetCommitMetadata` per unique commit — wall time within noise of the
   branch base, RSS steady. Update the history_scan example to print
   author + first message line.

## Escalation

None — proceed to implementation on confirmation.
