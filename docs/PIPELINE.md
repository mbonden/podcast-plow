# Pipeline overview

This project breaks the fact-checking workflow into three repeatable jobs. Each
stage reads from Postgres via the `DATABASE_URL` environment variable and writes
structured rows that downstream steps consume.

## Job catalogue

| Job | Module / CLI | Purpose | Primary outputs |
| --- | --- | --- | --- |
| Claim extraction | `python -m worker.claim_pipeline` | Parse transcripts into structured claims. | Replaces rows in `claim` for each processed episode. |
| Evidence fetcher | `python -m worker.worker evidence` | Search PubMed for supporting/contradicting literature. | Upserts rows in `evidence_source` and links them via `claim_evidence`. |
| Auto grader | `python -m worker.auto_grade` | Convert linked evidence into rubric grades. | Appends rows to `claim_grade` with `auto-v1` rationale. |

### Claim extraction pipeline

```
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/podcast_plow \
    python -m worker.claim_pipeline [--episode 123]
```

* Reads every transcript (or the optional `--episode` id) and extracts claims
  using the heuristics defined in `worker.claim_extraction`.
* Deletes existing claims for the episode so repeated runs stay idempotent.
* Inserts fresh claim rows with normalized text, topics, domains, timestamps and
  risk estimates derived from keyword lookups.

### Evidence fetcher

```
DATABASE_URL=... \
    python -m worker.worker evidence [--claim-id 42] [--min 3] [--max 10] [--force]
```

* Iterates over claims (all, or a single `--claim-id`) and builds PubMed query
  variants from normalized/raw claim text.
* Issues searches via `fetch_pubmed_articles`, ranks candidates by study
  quality/recency, and upserts the top hits into `evidence_source`.
* Links evidence to the claim in `claim_evidence`, tagging machine-generated
  notes that include the query context. Existing links are respected unless
  `--force` is provided.

### Auto grader

```
DATABASE_URL=... python -m worker.auto_grade
```

* Streams claims and their linked evidence via `worker.auto_grade.ClaimSource`.
* Computes deterministic grades with `server.core.grading.compute_grade`, which
  weighs supporting vs. refuting evidence and records the breakdown in the
  rationale string.
* Persists results in `claim_grade` (one row per run) so manual review can track
  grading history.

## Expected outputs

Running the jobs in order produces:

1. `worker.claim_pipeline` → populated `claim` rows for each transcript.
2. `worker.worker evidence` → curated evidence stored in `evidence_source` plus
   linkage metadata in `claim_evidence`.
3. `worker.auto_grade` → rubric-aligned grades in `claim_grade` referencing the
   supporting/refuting counts.

Each stage is safe to re-run. Extraction fully replaces prior claim rows for an
episode, the evidence fetcher upserts by PubMed ID/DOI, and the grader appends a
fresh history entry for every invocation.
