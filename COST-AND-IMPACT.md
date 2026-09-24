# Cost and environmental impact of producing this package

Scoped to the **implementation segment** — writing the files in `sql/`,
`airflow/`, and `README.md` — not the whole planning-plus-implementation effort.
All figures are read from what the harness recorded. Nothing here is estimated.

## Why this is not a subtraction

The plan called for `after − before` against
`.baseline/pre-implementation.json`. That baseline was captured **in the planning
session** (`856d3a86`, `$1.594` at capture time, `58,330` cumulative output
tokens). Implementation then ran in a **fresh session** (`beb19d6c`) created by
`/clear`, whose counters start at zero.

Subtracting across two independent sessions is meaningless — it produces a
negative number. So the implementation segment is measured as **the whole of
session `beb19d6c`**, which is exactly the segment: that session was opened for
this task and did nothing else.

## Measured

| | Value | Source |
|---|---|---|
| Segment cost | **$1.35** | `~/.claude/cost-report.sh --json` → `costUSD`, `source: statusline (live)` |
| Segment output tokens | **26,522** | session transcript, deduplicated by `.message.id` (17 distinct assistant messages) |
| API time | 314 s | `apiDurationMs` |
| Context at end of segment | 62,546 tokens (6% of 1M) | `contextTokens` |

**The dedup matters.** The raw sum of `output_tokens` across the transcript's 105
lines is **49,952** — the transcript repeats assistant messages, so a naive sum
overstates output by ~1.9×. The deduplicated 26,522 is what was fed to EcoLogits.

For context, not as the segment figure: cumulative spend across all 5 recorded
sessions in this project directory is **$7.34**, of which the planning session
(`856d3a86`) is **$2.94**.

## Environmental impact of the segment

`https://api.ecologits.ai/v1beta/estimations`, `provider=anthropic`,
`model_name=claude-opus-4-8`, `output_token_count=26522`,
`electricity_mix_zone=USA` — matching `~/.claude/ecologits.config.sh`. Midpoints
of the returned min/max ranges:

| Impact | Midpoint | Range |
|---|---|---|
| 🔥 Greenhouse gas (GWP) | **49 g CO₂eq** | 34 – 64 g |
| 💧 Water consumed (WCF) | **0.46 L** | 0.27 – 0.64 L |
| ⚡ Energy | **0.12 kWh** | 0.083 – 0.160 kWh |
| ⛏️ Mineral depletion (ADPe) | 0.15 mg Sbeq | 0.142 – 0.150 mg |
| 🛢️ Primary energy (PE) | 1.21 MJ | 0.84 – 1.58 MJ |

Two caveats on these numbers, both from the API itself:

- EcoLogits returned `model-arch-not-released` — Anthropic has not published the
  architecture, so precision is low. Treat the ranges, not the midpoints, as the
  real answer.
- The session ran on **Opus 5**, which EcoLogits does not yet list.
  `claude-opus-4-8` is the nearest available model, per the plan. The true figure
  for Opus 5 is unknown.

## Expected query-side savings (qualitative — not measured)

No query has been run, so there is no benchmark. What changes structurally, per
tile request:

**Removed from every request:**
- a recursive CTE walking the org hierarchy
- a `UNION` over two `trees` scans to resolve tree ownership
- **two** `HashAggregate`s over `active_tree_region` (the tile GROUP BY at one
  zoom level, and the `contained` GROUP BY at another)
- a full sort of the low-zoom × high-zoom cross product, discarded down to one
  row per region by `DISTINCT ON`

**What remains:** one index scan on `organization_clusters`
(`organization_id`, `zoom_level`, `centroid &&` — all three in one GiST index),
a PK probe on `region`, and a second index scan capped by `LIMIT 1` for the zoom
target. The per-request work stops scaling with tree count and hierarchy depth and
starts scaling with the number of clusters actually in the viewport.

**What it costs instead:** the hierarchy walk and both aggregations still happen —
four times a day, for every org, in the DAG. Whether that trade is favourable
depends on request volume versus the closure fan-out, and the fan-out is the
unknown: `organization_clusters` holds roughly (distinct region/zoom cells) ×
(average ancestors per org). Check 6 in `sql/99_sizing_checks.sql` measures it,
and check 6's own runtime is the refresh-cost estimate. **Run it before
deploying.**
