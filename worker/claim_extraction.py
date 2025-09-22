"""Utility functions for extracting structured claims from transcripts.

This module provides a lightweight heuristic implementation that is good
enough for bootstrapping an early claim extraction pipeline.  The focus is on
creating deterministic, testable behaviour rather than sophisticated natural
language understanding.

The extractor works in a couple of steps:

1.  Split the transcript into sentences and estimate timestamps using a
    constant words-per-minute assumption.  We then group the sentences into
    20–40 second segments.  Segments are useful for downstream systems (e.g.
    snippets or manual review) even if, for the purposes of this milestone,
    claims are derived at the sentence level.
2.  Filter sentences with a list of "claim verbs".  These verbs represent
    action-oriented statements that can be fact-checked (e.g. *improves* or
    *reduces*).  Anecdotal language such as "I remember" is ignored.
3.  Paraphrase the sentence by stripping filler phrases ("the guest says
    that"), applying a small synonym map, and wrapping it in a deterministic
    template ("The speaker maintains that …").
4.  Normalise the paraphrase so that we can deduplicate claims within an
    episode and provide a canonical form for search.
5.  Assign topic/domain labels and a risk estimate using keyword lookups.

The output of the extractor is a list of :class:`Claim` instances ready to be
inserted into the database.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable, List, Sequence, Tuple


# Speech rate assumption used for timestamp estimation.  120 wpm keeps the
# resulting segments within the 20–40 second window for typical paragraph
# lengths in our sample data.
WORDS_PER_MINUTE = 120
MS_PER_WORD = int(round(60_000 / WORDS_PER_MINUTE))

# Segmentation parameters (20–40 seconds)
SEGMENT_MIN_MS = 20_000
SEGMENT_MAX_MS = 40_000
SEGMENT_TARGET_MS = 30_000

# Key verbs indicating that a sentence is making a checkable assertion.
CLAIM_VERBS = {
    "increase",
    "improve",
    "reduce",
    "prevent",
    "support",
    "boost",
    "raise",
    "lower",
    "enhance",
    "maintain",
    "decrease",
    "assist",
    "protect",
    "strengthen",
    "fuel",
    "accelerate",
    "help",
    "shorten",
    "stabilize",
}

# Words/phrases that suggest the sentence is anecdotal rather than a testable
# claim.  We skip sentences that contain any of these tokens.
ANECDOTE_MARKERS = {
    "i remember",
    "i once",
    "i used to",
    "story",
    "my friend",
    "i feel",
    "i think",
}

# Mapping of keyword -> (topic, domain)
TOPIC_KEYWORDS: List[Tuple[str, str, str]] = [
    ("ketone", "ketones", "metabolism"),
    ("fast", "intermittent_fasting", "nutrition"),
    ("sleep", "sleep_quality", "wellness"),
    ("melatonin", "melatonin", "sleep"),
    ("circadian", "circadian_rhythm", "sleep"),
    ("cortisol", "stress_hormones", "endocrinology"),
    ("omega", "omega_3", "nutrition"),
    ("creatine", "creatine", "performance"),
    ("brown fat", "brown_adipose_tissue", "metabolism"),
    ("norepinephrine", "norepinephrine", "neurochemistry"),
    ("hydration", "hydration", "performance"),
    ("magnesium", "magnesium", "supplements"),
    ("microbiome", "gut_microbiome", "nutrition"),
    ("fermented", "fermented_foods", "nutrition"),
    ("probiotic", "probiotics", "nutrition"),
    ("glucose", "glucose_regulation", "metabolism"),
]

# Replacement pairs used to create simple paraphrases.  The replacements are
# case-insensitive and preserve deterministic wording.
PARAPHRASE_REPLACEMENTS: Sequence[Tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bboosts?\b", re.I), "elevates"),
    (re.compile(r"\bimproves?\b", re.I), "enhances"),
    (re.compile(r"\bincreases?\b", re.I), "raises"),
    (re.compile(r"\braises?\b", re.I), "raises"),
    (re.compile(r"\breduces?\b", re.I), "lowers"),
    (re.compile(r"\bdecreases?\b", re.I), "lowers"),
    (re.compile(r"\bhelps?\b", re.I), "assists"),
    (re.compile(r"\bsupports?\b", re.I), "supports"),
    (re.compile(r"\bprevents?\b", re.I), "avoids"),
    (re.compile(r"\bmaintains?\b", re.I), "maintains"),
    (re.compile(r"\bfuels?\b", re.I), "fuels"),
    (re.compile(r"\bprotects?\b", re.I), "protects"),
    (re.compile(r"\bshortens?\b", re.I), "shortens"),
]


@dataclass(frozen=True)
class Sentence:
    """A sentence with estimated timing metadata."""

    text: str
    start_word: int
    end_word: int
    start_ms: int
    end_ms: int


@dataclass(frozen=True)
class Segment:
    """A contiguous block of sentences roughly 20–40 seconds long."""

    text: str
    start_ms: int
    end_ms: int


@dataclass(frozen=True)
class Claim:
    """Structured claim data ready for persistence."""

    raw_text: str
    normalized_text: str
    topic: str
    domain: str
    risk_level: str
    start_ms: int
    end_ms: int


WORD_RE = re.compile(r"\b[\w']+\b")
SENTENCE_RE = re.compile(r"[^.!?]+[.!?]")


def iter_sentences(text: str) -> List[Sentence]:
    """Split *text* into sentences with approximate timing metadata."""

    tokens = list(WORD_RE.finditer(text))
    ms_per_word = MS_PER_WORD
    sentences: List[Sentence] = []

    if not tokens:
        return sentences

    token_index = 0
    for match in SENTENCE_RE.finditer(text):
        sentence_text = match.group().strip()
        start_char, end_char = match.span()

        # Advance to the first token that belongs to this sentence.
        while token_index < len(tokens) and tokens[token_index].end() <= start_char:
            token_index += 1
        start_word = token_index

        # Consume tokens contained in this sentence.
        while token_index < len(tokens) and tokens[token_index].start() < end_char:
            token_index += 1
        end_word = token_index

        if start_word == end_word:
            continue

        start_ms = start_word * ms_per_word
        end_ms = max(start_ms + ms_per_word, end_word * ms_per_word)
        sentences.append(
            Sentence(
                text=sentence_text,
                start_word=start_word,
                end_word=end_word,
                start_ms=start_ms,
                end_ms=end_ms,
            )
        )

    return sentences


def build_segments(sentences: Sequence[Sentence]) -> List[Segment]:
    """Group sentences into ~30s segments."""

    segments: List[Segment] = []
    if not sentences:
        return segments

    current: List[Sentence] = []
    seg_start_ms = sentences[0].start_ms

    for sentence in sentences:
        if not current:
            seg_start_ms = sentence.start_ms
        current.append(sentence)
        seg_end_ms = current[-1].end_ms
        duration = seg_end_ms - seg_start_ms

        should_close = False
        if duration >= SEGMENT_TARGET_MS:
            should_close = True
        elif duration >= SEGMENT_MIN_MS and len(current) >= 3:
            should_close = True

        if should_close:
            segments.append(
                Segment(
                    text=" ".join(s.text.strip() for s in current),
                    start_ms=seg_start_ms,
                    end_ms=seg_end_ms,
                )
            )
            current = []

    if current:
        segments.append(
            Segment(
                text=" ".join(s.text.strip() for s in current),
                start_ms=current[0].start_ms,
                end_ms=current[-1].end_ms,
            )
        )

    # Ensure final durations do not exceed the upper bound by splitting if
    # required.  This keeps the behaviour deterministic for testing.
    normalised_segments: List[Segment] = []
    for seg in segments:
        if seg.end_ms - seg.start_ms <= SEGMENT_MAX_MS or " " not in seg.text:
            normalised_segments.append(seg)
            continue

        sentence_texts = seg.text.split(". ")
        running_start = seg.start_ms
        for piece in sentence_texts:
            piece = piece.strip()
            if not piece:
                continue
            approx_words = len(piece.split())
            piece_duration = max(SEGMENT_MIN_MS, approx_words * MS_PER_WORD)
            running_end = min(running_start + piece_duration, seg.end_ms)
            normalised_segments.append(
                Segment(text=piece + ("." if not piece.endswith(".") else ""), start_ms=running_start, end_ms=running_end)
            )
            running_start = running_end

    return normalised_segments


def _looks_like_claim(sentence: Sentence) -> bool:
    lowered = sentence.text.lower()
    if any(marker in lowered for marker in ANECDOTE_MARKERS):
        return False
    return any(verb in lowered for verb in CLAIM_VERBS)


_LEADING_PHRASE = re.compile(
    r"^(?:(?:finally|additionally|overall|then|next|lastly)\s+)?"
    r"(?:(?:the\s+(?:host|guest|speaker|discussion))|(?:he|she|they|we))\s+"
    r"(?:(?:\w+\s+){0,2})?(?:states?|says?|notes?|mentions?|adds?|explains?|argues?|asserts?|comments?|observes?|reports?|believes|claims?|warns?|suggests?|emphasises?|concludes?)\s+"
    r"(?:that\s+)?",
    re.I,
)


def paraphrase(sentence: str) -> str:
    """Create a deterministic paraphrase of *sentence*."""

    text = sentence.strip()
    # Remove leading filler phrases ("The host says that ...").
    while True:
        new_text = _LEADING_PHRASE.sub("", text)
        if new_text == text:
            break
        text = new_text.strip()

    # Remove stray leading "that" produced by aggressive stripping.
    text = re.sub(r"^that\s+", "", text, flags=re.I)

    for pattern, replacement in PARAPHRASE_REPLACEMENTS:
        text = pattern.sub(replacement, text)

    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return ""

    if not text.endswith(('.', '!', '?')):
        text = f"{text}."

    core = text[0].lower() + text[1:] if len(text) > 1 else text.lower()
    return f"The speaker maintains that {core}"


def normalise(text: str) -> str:
    lowered = text.lower()
    lowered = re.sub(r"[^a-z0-9\s]", "", lowered)
    lowered = re.sub(r"\s+", " ", lowered)
    return lowered.strip()


def choose_topic_domain(normalized_text: str) -> Tuple[str, str]:
    for keyword, topic, domain in TOPIC_KEYWORDS:
        if keyword in normalized_text:
            return topic, domain
    return "general_health", "wellness"


def estimate_risk_level(normalized_text: str) -> str:
    if re.search(r"\b(?:cures?|eliminates|guarantees)\b", normalized_text):
        return "high"
    if re.search(r"\b(?:may|might|could|suggests?)\b", normalized_text):
        return "low"
    if re.search(r"\b(?:reduces?|lowers?|decreases?|improves?|enhances?|raises?|increases?)\b", normalized_text):
        return "medium"
    return "medium"


def extract_claims(text: str) -> List[Claim]:
    """Extract deterministic claims from *text*."""

    sentences = iter_sentences(text)
    segments = build_segments(sentences)

    claims: List[Claim] = []
    seen: set[str] = set()

    for sentence in sentences:
        if not _looks_like_claim(sentence):
            continue

        raw = paraphrase(sentence.text)
        if not raw:
            continue

        normalized = normalise(raw)
        if not normalized or normalized in seen:
            continue

        topic, domain = choose_topic_domain(normalized)
        risk = estimate_risk_level(normalized)

        claims.append(
            Claim(
                raw_text=raw,
                normalized_text=normalized,
                topic=topic,
                domain=domain,
                risk_level=risk,
                start_ms=sentence.start_ms,
                end_ms=sentence.end_ms,
            )
        )
        seen.add(normalized)

    # Ensure claims inherit segment boundaries if the estimated sentence times
    # happen to be zero-length (e.g., very short sentences).  We match the
    # sentence to the segment it belongs to.
    if segments:
        for idx, claim in enumerate(claims):
            for segment in segments:
                if segment.start_ms <= claim.start_ms < segment.end_ms:
                    if claim.end_ms < segment.start_ms:
                        continue
                    if claim.end_ms > segment.end_ms:
                        claim_end = segment.end_ms
                    else:
                        claim_end = claim.end_ms
                    claims[idx] = Claim(
                        raw_text=claim.raw_text,
                        normalized_text=claim.normalized_text,
                        topic=claim.topic,
                        domain=claim.domain,
                        risk_level=claim.risk_level,
                        start_ms=max(segment.start_ms, claim.start_ms),
                        end_ms=claim_end,
                    )
                    break

    return claims


def extract_claims_from_segments(segments: Iterable[Segment]) -> List[Claim]:
    """Convenience helper to process already segmented transcripts."""

    claims: List[Claim] = []
    for segment in segments:
        claims.extend(extract_claims(segment.text))
    return claims

