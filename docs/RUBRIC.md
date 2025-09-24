# Auto-grading rubric

The auto-grader assigns claim grades directly from the evidence rows linked in
Postgres. It classifies each evidence item's stance and study strength, then
aggregates the counts into one of four outcomes.

## Evidence strength buckets

* **High** – descriptors such as *meta-analysis*, *systematic review*,
  *randomized controlled trial*, *randomized*, *double-blind*, or *RCT*.
* **Medium** – phrases including *cohort*, *case-control*, *observational*,
  *clinical trial*, *pilot*, *survey*, or the generic word *study*.
* **Low** – weaker signals like *case report*, *case series*, *animal*,
  *mechanistic*, *in vitro*, *cell*, or *expert opinion*.

Unrecognized study labels default to medium quality so they count toward a grade
without inflating confidence.

## Grade definitions

* **Strong** – At least two high-quality supporting items, or one high-quality
  support backed by either another medium-quality support or three total
  supporting pieces.
* **Moderate** – One high-quality support (without satisfying the "strong"
  criteria) or at least two medium-quality supports.
* **Weak** – Any remaining evidence with at least one medium- or low-strength
  supporting item.
* **Unsupported** – No supporting evidence linked to the claim.

## Handling conflicting evidence

Refuting evidence reduces confidence after the initial grade is calculated:

* Any high-strength refutation steps the grade down by two levels.
* Any medium- or low-strength refutation steps the grade down by one level.

The stored rationale strings explain the supporting and refuting counts, and add
"Conflicting evidence reduced confidence." whenever a downgrade occurs.

## Validation

Unit tests in `tests/test_auto_grade.py` lock the grading behaviour in place:

* `compute_grade` scenarios check each bucket, including conflict handling.
* `AutoGradeService` is exercised against the fake database to ensure re-grading
  appends a fresh `claim_grade` row with the current `rubric_version`.
