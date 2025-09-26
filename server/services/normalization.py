"""Utilities for normalizing claim metadata."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Dict


def _module_path(filename: str) -> Path:
    return Path(__file__).with_name(filename)


def _load_map(filename: str) -> Dict[str, str]:
    path = _module_path(filename)
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)

    alias_map: Dict[str, str] = {}
    for canonical, aliases in raw.items():
        if canonical is None:
            continue
        canonical_clean = str(canonical).strip()
        if not canonical_clean:
            continue
        alias_map[canonical_clean.lower()] = canonical_clean
        if isinstance(aliases, str):
            iterable = [aliases]
        else:
            iterable = aliases or []
        for alias in iterable:
            if not alias:
                continue
            alias_clean = str(alias).strip()
            if not alias_clean:
                continue
            alias_map[alias_clean.lower()] = canonical_clean
    return alias_map


@lru_cache(maxsize=1)
def _topic_map() -> Dict[str, str]:
    return _load_map("topic_map.json")


@lru_cache(maxsize=1)
def _domain_map() -> Dict[str, str]:
    return _load_map("domain_map.json")


def _canonicalize(value: str | None, lookup: Dict[str, str]) -> str:
    if value is None:
        return ""
    cleaned = value.strip()
    if not cleaned:
        return ""
    canonical = lookup.get(cleaned.lower())
    if canonical:
        return canonical
    return cleaned


def canonical_topic(topic: str | None) -> str:
    """Return the canonical topic label for *topic* if configured."""

    return _canonicalize(topic, _topic_map())


def canonical_domain(domain: str | None) -> str:
    """Return the canonical domain label for *domain* if configured."""

    return _canonicalize(domain, _domain_map())


__all__ = ["canonical_topic", "canonical_domain"]
