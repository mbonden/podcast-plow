from __future__ import annotations

from fastapi.testclient import TestClient

import server.app as app_module

from tests.test_api_privacy_and_claims import fake_db, seeded_client


def test_claim_endpoint_ranks_limits_and_dedupes_evidence(
    seeded_client: TestClient,
) -> None:
    with app_module.db_conn() as conn:
        cur = conn.cursor()
        records = [
            (
                "Systematic Review on Ketones",
                2024,
                "10.1000/systematic",
                "9990001",
                "https://example.org/systematic",
                "systematic review",
                "Annual Reviews",
            ),
            (
                "Updated Meta-analysis on Ketogenic Diets",
                2022,
                "10.1000/meta-updated",
                "9990002",
                "https://example.org/meta-updated",
                "meta-analysis",
                "Nutrients",
            ),
            (
                "Randomized Trial of Exogenous Ketones",
                2021,
                "10.1000/rct-shakes",
                "9990003",
                "https://example.org/rct",
                "RCT",
                "Trials",
            ),
            (
                "Duplicate Randomized Trial of Exogenous Ketones",
                2020,
                "10.1000/rct-shakes",
                "9990003",
                "https://example.org/rct-duplicate",
                "RCT",
                "Trials",
            ),
            (
                "Observational Study of Ketone Supplement Users",
                2023,
                "10.1000/observational-a",
                "9990004",
                "https://example.org/observational-a",
                "observational",
                "Journal of Metabolism",
            ),
            (
                "Cohort Study of Endogenous Ketone Levels",
                2020,
                "10.1000/observational-b",
                "9990005",
                "https://example.org/observational-b",
                "observational",
                "Lancet Metabolism",
            ),
            (
                "Mechanistic Insight into Ketone Transport",
                2019,
                None,
                None,
                "https://example.org/mechanistic",
                "mechanistic",
                "Cell Reports",
            ),
            (
                "Molecular Pathways of Ketone Utilisation",
                2024,
                None,
                None,
                "https://example.org/mechanistic-two",
                "mechanistic",
                "Nature Metabolism",
            ),
        ]
        for title, year, doi, pubmed_id, url, evidence_type, journal in records:
            cur.execute(
                """
                INSERT INTO evidence_source (title, year, doi, pubmed_id, url, type, journal)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (title, year, doi, pubmed_id, url, evidence_type, journal),
            )
            evidence_id = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO claim_evidence (claim_id, evidence_id, stance) VALUES (%s, %s, %s)",
                (1, evidence_id, "supports"),
            )

    response = seeded_client.get("/claims/1")
    assert response.status_code == 200
    payload = response.json()

    evidence = payload.get("evidence", [])
    assert len(evidence) == 6

    titles = [item["title"] for item in evidence]

    assert titles[:2] == [
        "Systematic Review on Ketones",
        "Example Meta-analysis on Ketogenic Diets",
    ]
    assert "Duplicate Randomized Trial of Exogenous Ketones" not in titles
    assert "Molecular Pathways of Ketone Utilisation" not in titles

    assert [item["is_primary"] for item in evidence[:2]] == [True, True]
    assert all(not item["is_primary"] for item in evidence[2:])

    assert any(item["type"] == "RCT" for item in evidence)
    assert sum(1 for item in evidence if item["type"] == "observational") == 2
