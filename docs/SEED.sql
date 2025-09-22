-- Example seeds (run inside psql)
INSERT INTO podcast (title) VALUES ('The Tim Ferriss Show');
INSERT INTO episode (podcast_id, title, published_at, spotify_id) VALUES (1, 'Ep. 825 - Dominic D’Agostino', now(), '7fdiy8pEI8mZhgLeQzfAPb');
INSERT INTO episode_summary (episode_id, tl_dr, narrative, created_by) VALUES (1, 'Ketones, creatine, pragmatic levers.', 'Longer narrative...', 'human');
INSERT INTO claim (episode_id, raw_text, normalized_text, topic, domain, risk_level) VALUES
 (1, 'Ketones can support cognition.', 'ketones improve cognition', 'ketones', 'neuro', 'low');
INSERT INTO evidence_source (title, year, doi, pubmed_id, url, type, journal) VALUES
 ('Example Meta-analysis on Ketogenic Diets', 2023, '10.1000/example', '12345678', 'https://pubmed.ncbi.nlm.nih.gov/12345678/', 'meta-analysis', 'Example Journal');
INSERT INTO claim_evidence (claim_id, evidence_id, stance) VALUES (1, 1, 'supports');
INSERT INTO claim_grade (claim_id, grade, rationale, graded_by) VALUES (1, 'moderate', 'Human RCTs exist but limited in size.', 'mbonden');
