-- Sample podcast, episodes, transcripts, and claims for development/testing.

INSERT INTO podcast (id, title, description)
VALUES
    (1, 'Metabolic Morning Show', 'A fictional wellness podcast used for integration testing')
ON CONFLICT (id) DO NOTHING;

INSERT INTO podcast (id, title, description)
VALUES
    (2, 'Brain and Body Chat', 'Another fictional show for sample data')
ON CONFLICT (id) DO NOTHING;

INSERT INTO episode (id, podcast_id, title, description, published_at, duration_sec)
VALUES
    (1, 1, 'Metabolic Morning Show 001', 'Metabolic routines for morning energy', '2024-01-05', 1800),
    (2, 1, 'Metabolic Morning Show 002', 'Cold exposure and training insights', '2024-01-12', 1900),
    (3, 2, 'Brain and Body Chat 015', 'Nutrition and sleep tactics for cognition', '2024-01-19', 2000)
ON CONFLICT (id) DO NOTHING;

INSERT INTO transcript (id, episode_id, source, lang, text, word_count, has_verbatim_ok)
VALUES
    (1, 1, 'synthetic', 'en', 'This morning show host explains that keeping blood sugar stable reduces afternoon crashes. He says a consistent overnight fast of fourteen hours raises ketone production and supports morning focus. She claims that drinking mineral water with sodium prevents headaches during fasting. He notes research showing that a protein rich breakfast lowers cravings for sugary snacks. They discuss how a quick walk in daylight improves sleep onset that night. The guest argues that limiting blue light after sunset increases melatonin and sleep depth. The host states that practicing box breathing for five minutes decreases cortisol levels. She adds that adding a tablespoon of ground flaxseed daily improves digestion. He mentions that consistent bed times calibrate the circadian rhythm. They assert that journaling gratitude each evening reduces perceived stress. She reports that replacing afternoon coffee with herbal tea reduces evening anxiety. Finally he says that using a cool bedroom around sixty five degrees improves sleep quality.', 154, TRUE),
    (2, 2, 'synthetic', 'en', 'The host asserts that a ten minute cold shower boosts norepinephrine for hours. He states that moderate cold exposure three times per week increases brown fat activity. She explains that combining cold plunges with breathing exercises accelerates recovery after workouts. The guest says that eating salmon twice a week raises omega three levels that support heart health. He claims that omega three fats reduce inflammation markers in endurance athletes. She adds that supplementing creatine improves power output during sprint training. He notes that creatine supplementation also supports cognitive resilience during sleep deprivation. They mention that pairing creatine with carbohydrates enhances uptake into muscle cells. The host comments that practicing mindfulness before training lowers perceived exertion. She says that stretching hips daily reduces lower back pain in runners. He adds that rotating running shoes decreases injury risk. They conclude that going to bed before midnight improves hormone regulation. Finally he remarks that cold exposure combined with fasting keeps ketone levels elevated.', 160, TRUE),
    (3, 3, 'synthetic', 'en', 'The neuroscientist guest claims that maintaining steady ketone availability fuels brain metabolism when glucose dips. She states that exogenous ketone drinks can enhance mental endurance during long study sessions. He says that periodic fasting improves insulin sensitivity in middle aged adults. The host notes that pairing fasting with light exercise increases fat oxidation. She observes that consuming fermented vegetables daily supports gut microbiome diversity. He mentions that probiotics reduce the frequency of seasonal colds. They suggest that adequate hydration before workouts prevents dizziness. The guest remarks that magnesium glycinate before bed improves sleep efficiency. He adds that dimming lights an hour before bed lowers evening cortisol. She claims that listening to calming music before sleep shortens sleep latency. He warns that scrolling on phones at night delays melatonin release. They finish by saying that consistent wake times strengthen circadian alignment.', 140, TRUE)
ON CONFLICT (id) DO NOTHING;

-- Claims for episode 1
INSERT INTO claim (id, episode_id, start_ms, end_ms, raw_text, normalized_text, topic, domain, risk_level)
VALUES
    (1, 1, 0, 6500, 'The speaker maintains that this morning show host explains that keeping blood sugar stable lowers afternoon crashes.', 'the speaker maintains that this morning show host explains that keeping blood sugar stable lowers afternoon crashes', 'general_health', 'wellness', 'medium'),
    (2, 1, 6500, 14500, 'The speaker maintains that a consistent overnight fast of fourteen hours raises ketone production and supports morning focus.', 'the speaker maintains that a consistent overnight fast of fourteen hours raises ketone production and supports morning focus', 'ketones', 'metabolism', 'medium'),
    (3, 1, 14500, 20500, 'The speaker maintains that drinking mineral water with sodium avoids headaches during fasting.', 'the speaker maintains that drinking mineral water with sodium avoids headaches during fasting', 'intermittent_fasting', 'nutrition', 'medium'),
    (4, 1, 20500, 27500, 'The speaker maintains that research showing that a protein rich breakfast lowers cravings for sugary snacks.', 'the speaker maintains that research showing that a protein rich breakfast lowers cravings for sugary snacks', 'intermittent_fasting', 'nutrition', 'medium'),
    (5, 1, 27500, 34000, 'The speaker maintains that they discuss how a quick walk in daylight enhances sleep onset that night.', 'the speaker maintains that they discuss how a quick walk in daylight enhances sleep onset that night', 'sleep_quality', 'wellness', 'medium'),
    (6, 1, 34000, 41000, 'The speaker maintains that limiting blue light after sunset raises melatonin and sleep depth.', 'the speaker maintains that limiting blue light after sunset raises melatonin and sleep depth', 'sleep_quality', 'wellness', 'medium'),
    (7, 1, 41000, 47500, 'The speaker maintains that practicing box breathing for five minutes lowers cortisol levels.', 'the speaker maintains that practicing box breathing for five minutes lowers cortisol levels', 'stress_hormones', 'endocrinology', 'medium'),
    (8, 1, 47500, 53500, 'The speaker maintains that adding a tablespoon of ground flaxseed daily enhances digestion.', 'the speaker maintains that adding a tablespoon of ground flaxseed daily enhances digestion', 'general_health', 'wellness', 'medium'),
    (9, 1, 58500, 63500, 'The speaker maintains that journaling gratitude each evening lowers perceived stress.', 'the speaker maintains that journaling gratitude each evening lowers perceived stress', 'general_health', 'wellness', 'medium'),
    (10, 1, 63500, 69500, 'The speaker maintains that replacing afternoon coffee with herbal tea lowers evening anxiety.', 'the speaker maintains that replacing afternoon coffee with herbal tea lowers evening anxiety', 'general_health', 'wellness', 'medium'),
    (11, 1, 69500, 77000, 'The speaker maintains that using a cool bedroom around sixty five degrees enhances sleep quality.', 'the speaker maintains that using a cool bedroom around sixty five degrees enhances sleep quality', 'sleep_quality', 'wellness', 'medium')
ON CONFLICT (id) DO NOTHING;

-- Claims for episode 2
INSERT INTO claim (id, episode_id, start_ms, end_ms, raw_text, normalized_text, topic, domain, risk_level)
VALUES
    (12, 2, 0, 6500, 'The speaker maintains that a ten minute cold shower elevates norepinephrine for hours.', 'the speaker maintains that a ten minute cold shower elevates norepinephrine for hours', 'norepinephrine', 'neurochemistry', 'medium'),
    (13, 2, 6500, 13500, 'The speaker maintains that moderate cold exposure three times per week raises brown fat activity.', 'the speaker maintains that moderate cold exposure three times per week raises brown fat activity', 'brown_adipose_tissue', 'metabolism', 'medium'),
    (14, 2, 13500, 20000, 'The speaker maintains that combining cold plunges with breathing exercises accelerates recovery after workouts.', 'the speaker maintains that combining cold plunges with breathing exercises accelerates recovery after workouts', 'general_health', 'wellness', 'medium'),
    (15, 2, 20000, 28500, 'The speaker maintains that eating salmon twice a week raises omega three levels that supports heart health.', 'the speaker maintains that eating salmon twice a week raises omega three levels that supports heart health', 'omega_3', 'nutrition', 'medium'),
    (16, 2, 28500, 34500, 'The speaker maintains that omega three fats lowers inflammation markers in endurance athletes.', 'the speaker maintains that omega three fats lowers inflammation markers in endurance athletes', 'omega_3', 'nutrition', 'medium'),
    (17, 2, 34500, 40000, 'The speaker maintains that supplementing creatine enhances power output during sprint training.', 'the speaker maintains that supplementing creatine enhances power output during sprint training', 'creatine', 'performance', 'medium'),
    (18, 2, 40000, 46000, 'The speaker maintains that creatine supplementation also supports cognitive resilience during sleep deprivation.', 'the speaker maintains that creatine supplementation also supports cognitive resilience during sleep deprivation', 'sleep_quality', 'wellness', 'medium'),
    (19, 2, 46000, 52000, 'The speaker maintains that pairing creatine with carbohydrates enhances uptake into muscle cells.', 'the speaker maintains that pairing creatine with carbohydrates enhances uptake into muscle cells', 'creatine', 'performance', 'medium'),
    (20, 2, 52000, 57500, 'The speaker maintains that practicing mindfulness before training lowers perceived exertion.', 'the speaker maintains that practicing mindfulness before training lowers perceived exertion', 'general_health', 'wellness', 'medium'),
    (21, 2, 57500, 63500, 'The speaker maintains that stretching hips daily lowers lower back pain in runners.', 'the speaker maintains that stretching hips daily lowers lower back pain in runners', 'general_health', 'wellness', 'medium'),
    (22, 2, 63500, 68000, 'The speaker maintains that rotating running shoes lowers injury risk.', 'the speaker maintains that rotating running shoes lowers injury risk', 'general_health', 'wellness', 'medium'),
    (23, 2, 68000, 73500, 'The speaker maintains that going to bed before midnight enhances hormone regulation.', 'the speaker maintains that going to bed before midnight enhances hormone regulation', 'general_health', 'wellness', 'medium')
ON CONFLICT (id) DO NOTHING;

-- Claims for episode 3
INSERT INTO claim (id, episode_id, start_ms, end_ms, raw_text, normalized_text, topic, domain, risk_level)
VALUES
    (24, 3, 0, 7500, 'The speaker maintains that the neuroscientist guest claims that maintaining steady ketone availability fuels brain metabolism when glucose dips.', 'the speaker maintains that the neuroscientist guest claims that maintaining steady ketone availability fuels brain metabolism when glucose dips', 'ketones', 'metabolism', 'medium'),
    (25, 3, 7500, 14500, 'The speaker maintains that exogenous ketone drinks can enhance mental endurance during long study sessions.', 'the speaker maintains that exogenous ketone drinks can enhance mental endurance during long study sessions', 'ketones', 'metabolism', 'medium'),
    (26, 3, 14500, 20500, 'The speaker maintains that periodic fasting enhances insulin sensitivity in middle aged adults.', 'the speaker maintains that periodic fasting enhances insulin sensitivity in middle aged adults', 'intermittent_fasting', 'nutrition', 'medium'),
    (27, 3, 20500, 26500, 'The speaker maintains that pairing fasting with light exercise raises fat oxidation.', 'the speaker maintains that pairing fasting with light exercise raises fat oxidation', 'intermittent_fasting', 'nutrition', 'medium'),
    (28, 3, 26500, 32000, 'The speaker maintains that consuming fermented vegetables daily supports gut microbiome diversity.', 'the speaker maintains that consuming fermented vegetables daily supports gut microbiome diversity', 'gut_microbiome', 'nutrition', 'medium'),
    (29, 3, 32000, 37000, 'The speaker maintains that probiotics lowers the frequency of seasonal colds.', 'the speaker maintains that probiotics lowers the frequency of seasonal colds', 'probiotics', 'nutrition', 'medium'),
    (30, 3, 37000, 41500, 'The speaker maintains that adequate hydration before workouts avoids dizziness.', 'the speaker maintains that adequate hydration before workouts avoids dizziness', 'hydration', 'performance', 'medium'),
    (31, 3, 41500, 47000, 'The speaker maintains that the guest remarks that magnesium glycinate before bed enhances sleep efficiency.', 'the speaker maintains that the guest remarks that magnesium glycinate before bed enhances sleep efficiency', 'sleep_quality', 'wellness', 'medium'),
    (32, 3, 47000, 53000, 'The speaker maintains that dimming lights an hour before bed lowers evening cortisol.', 'the speaker maintains that dimming lights an hour before bed lowers evening cortisol', 'stress_hormones', 'endocrinology', 'medium'),
    (33, 3, 53000, 59000, 'The speaker maintains that listening to calming music before sleep shortens sleep latency.', 'the speaker maintains that listening to calming music before sleep shortens sleep latency', 'sleep_quality', 'wellness', 'medium'),
    (34, 3, 64500, 70000, 'The speaker maintains that they finish by saying that consistent wake times strengthen circadian alignment.', 'the speaker maintains that they finish by saying that consistent wake times strengthen circadian alignment', 'circadian_rhythm', 'sleep', 'medium')
ON CONFLICT (id) DO NOTHING;


SELECT setval('podcast_id_seq', COALESCE((SELECT MAX(id) FROM podcast), 0), true);
SELECT setval('episode_id_seq', COALESCE((SELECT MAX(id) FROM episode), 0), true);
SELECT setval('transcript_id_seq', COALESCE((SELECT MAX(id) FROM transcript), 0), true);
SELECT setval('claim_id_seq', COALESCE((SELECT MAX(id) FROM claim), 0), true);
