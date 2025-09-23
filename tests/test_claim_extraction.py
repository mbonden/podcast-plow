import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from worker.claim_extraction import (
    SEGMENT_MAX_MS,
    SEGMENT_MIN_MS,
    build_segments,
    extract_claims,
    iter_sentences,
)


@pytest.fixture(scope="module")
def sample_episodes():
    data_path = Path(__file__).parent / "data" / "sample_transcripts.json"
    with data_path.open() as fh:
        return json.load(fh)["episodes"]


def test_each_episode_has_claims(sample_episodes):
    for episode in sample_episodes:
        claims = extract_claims(episode["transcript"])
        assert claims, "expected claim extraction to return at least one claim"

        normalized = [claim.normalized_text for claim in claims]
        assert len(set(normalized)) == len(normalized)
        assert all(claim.start_ms < claim.end_ms for claim in claims)


def test_topics_overlap_across_episodes(sample_episodes):
    topic_map: dict[str, set[int]] = {}
    for idx, episode in enumerate(sample_episodes):
        for claim in extract_claims(episode["transcript"]):
            topic_map.setdefault(claim.topic, set()).add(idx)

    overlapping = [episodes for episodes in topic_map.values() if len(episodes) >= 2]
    assert overlapping, "expected at least one topic to appear in multiple episodes"


def test_segment_duration_bounds(sample_episodes):
    # Use the first episode as a representative sample
    episode = sample_episodes[0]
    sentences = iter_sentences(episode["transcript"])
    segments = build_segments(sentences)
    assert segments, "segments should not be empty"

    for idx, segment in enumerate(segments):
        duration = segment.end_ms - segment.start_ms
        assert duration <= SEGMENT_MAX_MS
        if idx < len(segments) - 1:
            assert duration >= SEGMENT_MIN_MS

