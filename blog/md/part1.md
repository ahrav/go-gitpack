# 🔓 Under the Hood: Unpacking Git's Secrets

**Part 1: Why We’re Building Our *****Own***** Git Engine**

When you run a secret‑scanner over a Git repo you really want **two things**:

1. **Speed** – so nightly scans don’t turn into weekly scans.
2. **Focus** – so you examine *only* the lines that were *added* (the “+” lines).

Our matching engine is already pretty fast leveraging techniques like Aho‑Corasick. Yet on huge monorepos the *overall* run‑time is dominated by one innocent‑looking command:

```bash
git log -p --all
```

`git log -p` walks every commit, decompresses every object and computes a diff just to print a human‑friendly patch. We immediately discard \~ 50 % of what it prints (all the “-” lines) and pipe the rest into the scanner.

> ❓ **Can’t we just talk to Git’s *****database***** directly and skip the middle‑man?**

Spoiler: **Yes – and it’s surprisingly straightforward once you know where the bytes live.**

---

## Meet Git’s Low‑Level Storage

| File                         | Think of it as                                                           | Why we need it                        |
| ---------------------------- | ------------------------------------------------------------------------ | ------------------------------------- |
| `*.pack`                     | a *zip* that holds **all** objects, many stored as space‑saving *deltas* | holds the actual bytes we’ll scan     |
| `*.idx`                      | a *table of contents* for *one* pack                                     | lets us jump to an object in O(1)     |
| `multi-pack-index` (`.midx`) | a *global* TOC when you have dozens of packs                             | keeps look‑ups fast in gigantic repos |

---

### Extra accelerators we’ll also parse

| File                              | Think of it as                                               | Why we need it                                                                                                                                               |
| --------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `pack-*.bitmap`                   | a *cheat‑sheet* of which objects are reachable from each ref | lets us enumerate commit ranges in **O(objects\_changed)** instead of **O(all\_objects)** when we only care about “changes since last scan”                  |
| `*.rev` / `.ridx` (reverse‑index) | the *inverse* mapping: *pack offset → object hash*           | required whenever we need to go **from a pack offset back to its SHA**—crucial for delta resolution and integrity checks without rescanning the entire index |

## The 5‑Step Express Lane from *Index* to *Diff*

Instead of asking Git to do everything, we split the work into five razor‑focused stages:

1. **Index lookup** – Map *object‑hash → (pack‑file, offset, crc)* using the `.idx` or the single `.midx` file. **Why?** This gives us *O(1) random access* to any object; every later stage can jump straight to the right bytes instead of linearly scanning gigabytes of storage.
2. **Inflate / Delta resolve** – Read the bytes at that offset, apply any *delta chain* (a series of space‑saving patches that, when applied in order, rebuilds the original file), and zlib‑decompress until we have the *full* object. **Why?** Packfiles store many objects as compressed deltas, until we rebuild them we don’t have real data to work with.
3. **Semantic parsing** – Turn raw bytes into structs: `Commit{Parents…}`, `Tree{entries…}` or `Blob{content}`. **Why?** Structured objects expose commit relationships and directory listings that the graph walker and diff engine depend on.
4. **Commit‑graph walk** – Depth‑first or breadth‑first traversal that visits every commit once and yields `(tree_now, tree_parent)` pairs. **Why?** Produces an ordered stream of snapshot pairs so we know exactly *which two trees* to compare, while guaranteeing no commit is processed twice.
5. **Tree + blob diff** – Compare those trees, grab changed blobs, compute a unified diff, and keep only the `+` lines for the scanner. **Why?** Isolates just the newly added code, shrinking the scanner’s workload to the bare minimum.

> 🏎️ \*\*Why this beats \*\*\`\` *Every stage does one cheap thing and passes a tiny, purpose‑built data structure to the next.* No shelling out, no human‑format text, no wasted bytes.

---

## Roadmap for the Series

| Part | What we’ll build                                                       | Related stage |
| ---- | ---------------------------------------------------------------------- | ------------- |
| 2    | **Pack Index Parser** – decode fan‑out table, binary search SHAs       | 1             |
| 3    | **Multi‑Pack‑Index Support** – O(1) look‑ups across many packs         | 1             |
| 4    | **Packfile Reader & Delta Resolver** – zlib + Git’s patch opcodes      | 2             |
| 5    | **Commit / Tree Parsing** – tiny line scanners, no regex               | 3             |
| 6    | **Commit‑Graph Walker** – DFS, BFS & incremental scans                 | 4             |
| 7    | **Tree & Blob Diff** – Myers diff for additions only                   | 5             |
| 8    | **Pipeline Plug‑in & Benchmarks** – replace `git log`, measure the win | all           |

---

## Glimpse of the Pay‑Off

| Metric (Linux kernel mirror) | `git log -p`      | Our pipeline |
| ---------------------------- | ----------------- | ------------ |
| Hunks processed per second   | \~ 3 k            | **30 k +**   |
| Syscalls per scan            | \~ 30 000         | **< 1000**   |
| External binaries needed     | `git`, `pager`, … | **none**     |

A *single‑binary* scanner that streams packs straight from GitHub’s API and finishes 10× faster? – *That’s* the ride we’re on.

---

### Up Next

In **Part 2** we crack open the `.idx` format, build a lightning‑fast lookup table, and write the very first line of Go that peeks inside a packfile.

Stay tuned, and happy (secret) hunting! 🚀
