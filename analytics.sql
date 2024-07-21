-- Count the number of animes in each genre
SELECT
    g.id AS genre_id,
    g.name AS genre_name,
    COUNT(ag.anime_id) AS anime_count
FROM
    public.genre g
    JOIN public.anime_genre ag ON g.id = ag.genre_id
GROUP BY
    g.id,
    g.name
ORDER BY
    anime_count DESC;

-- Count the number of mangas in each genre
SELECT
    g.id AS genre_id,
    g.name AS genre_name,
    COUNT(mg.manga_id) AS manga_count
FROM
    public.genre g
    JOIN public.manga_genre mg ON g.id = mg.genre_id
GROUP BY
    g.id,
    g.name
ORDER BY
    manga_count DESC;


-- Count the number of animes in each studio
WITH studio_anime_counts AS (
    SELECT
        a_studio.studio_id,
        COUNT(DISTINCT a_studio.anime_id) AS anime_count
    FROM
        public.anime_studio a_studio
    JOIN
        public.anime a ON a.id = a_studio.anime_id
    GROUP BY
        a_studio.studio_id
)
SELECT
    s.id AS studio_id,
    s.name AS studio_name,
    COALESCE(sac.anime_count, 0) AS anime_count
FROM
    public.studio s
LEFT JOIN
    studio_anime_counts sac ON s.id = sac.studio_id
ORDER BY
    anime_count DESC;



-- Top 10 most listed anime by users
SELECT
  a.id AS anime_id,
  (a.title -> 0 ->> 'original') AS original_title,
  (a.title -> 1 ->> 'en') AS en_title,
  (a.title -> 2 ->> 'jp') AS jp_title,
  s.users_listed
FROM
  public.anime a
  JOIN public.anime_statistics a_stat ON a.id = a_stat.anime_id
  JOIN public.statistics s ON a_stat.statistics_id = s.id
ORDER BY
  s.users_listed DESC
LIMIT
  10;


-- Top 10 most listed manga by users
SELECT
  m.id AS manga_id,
  (m.title -> 0 ->> 'original') AS original_title,
  (m.title -> 1 ->> 'en') AS en_title,
  (m.title -> 2 ->> 'jp') AS jp_title,
  s.users_listed
FROM
  public.manga m
  JOIN public.manga_statistics m_stat ON m.id = m_stat.manga_id
  JOIN public.statistics s ON m_stat.statistics_id = s.id
ORDER BY
  s.users_listed DESC
LIMIT
  10;


-- Top 10 highest rated animes by users
SELECT 
    a.id AS anime_id,
    (a.title -> 0 ->> 'original') AS original_title,
    (a.title -> 1 ->> 'en') AS en_title,
    (a.title -> 2 ->> 'jp') AS jp_title,
    s.mean
FROM 
    public.statistics s
JOIN 
    public.anime a
ON 
    s.id = a.id AND s.endpoint = 'anime'
WHERE 
    s.mean IS NOT NULL 
ORDER BY 
    s.mean DESC
LIMIT 10;

-- Top 10 highest rated mangas by users
SELECT 
    m.id AS manga_id,
    (m.title -> 0 ->> 'original') AS original_title,
    (m.title -> 1 ->> 'en') AS en_title,
    (m.title -> 2 ->> 'jp') AS jp_title,
    s.mean
FROM 
    public.statistics s
JOIN 
    public.manga m
ON 
    s.id = m.id AND s.endpoint = 'manga'
WHERE 
    s.mean IS NOT NULL 
ORDER BY 
    s.mean DESC
LIMIT 10;

-- Top 10 most completed anime by users
SELECT
  a.id AS anime_id,
  (a.title -> 0 ->> 'original') AS original_title,
  (a.title -> 1 ->> 'en') AS en_title,
  (a.title -> 2 ->> 'jp') AS jp_title,
  s.completed,
  s.watching,
  s.on_hold,
  s.dropped,
  s.plan_to_watch
FROM
  public.anime a
  JOIN public.anime_statistics a_stat ON a.id = a_stat.anime_id
  JOIN public.statistics s ON a_stat.statistics_id = s.id
ORDER BY
  s.completed DESC NULLS LAST
LIMIT
  10;


-- Top 10 longest aired anime
SELECT
	id AS anime_id,
	(title -> 0 ->> 'original') AS original_title,
	(title -> 1 ->> 'en') AS en_title,
	(title -> 2 ->> 'jp') AS jp_title,
	start_date,
	end_date,
	(end_date - start_date) / 365.25 AS duration_years
FROM
	public.anime
WHERE
	start_date IS NOT NULL
	AND end_date IS NOT NULL
ORDER BY
	duration_years DESC
LIMIT 10;


-- Top 10 longest aired manga
SELECT
	id AS manga_id,
	(title -> 0 ->> 'original') AS original_title,
	(title -> 1 ->> 'en') AS en_title,
	(title -> 2 ->> 'jp') AS jp_title,
	start_date,
	end_date,
	(end_date - start_date) / 365.25 AS duration_years
FROM
	public.manga
WHERE
	start_date IS NOT NULL
	AND end_date IS NOT NULL
ORDER BY
	duration_years DESC
LIMIT 10;

-- Top 10 animes by mean 
SELECT
  a.id AS anime_id,
  (a.title -> 0 ->> 'original') AS original_title,
  (a.title -> 1 ->> 'en') AS en_title,
  (a.title -> 2 ->> 'jp') AS jp_title,
  s.mean
FROM
  public.anime a
  JOIN public.anime_statistics a_stat ON a.id = a_stat.anime_id
  JOIN public.statistics s ON a_stat.statistics_id = s.id
WHERE
  s.mean IS NOT NULL
ORDER BY
  s.mean DESC
LIMIT
  10;

-- Top 10 mangas by mean
SELECT
  m.id AS manga_id,
  (m.title -> 0 ->> 'original') AS original_title,
  (m.title -> 1 ->> 'en') AS en_title,
  (m.title -> 2 ->> 'jp') AS jp_title,
  s.mean
FROM
  public.manga m
  JOIN public.manga_statistics m_stat ON m.id = m_stat.manga_id
  JOIN public.statistics s ON m_stat.statistics_id = s.id
WHERE
  s.mean IS NOT NULL
ORDER BY
  s.mean DESC
LIMIT
  10;


-- Top 10 longest anime titles
SELECT
  a.id AS anime_id,
  CASE
    WHEN LENGTH(a.title -> 0 ->> 'original') = GREATEST(
      LENGTH(a.title -> 0 ->> 'original'),
      LENGTH(a.title -> 1 ->> 'en'),
      LENGTH(a.title -> 2 ->> 'jp')
    ) THEN a.title -> 0 ->> 'original'
    WHEN LENGTH(a.title -> 1 ->> 'en') = GREATEST(
      LENGTH(a.title -> 0 ->> 'original'),
      LENGTH(a.title -> 1 ->> 'en'),
      LENGTH(a.title -> 2 ->> 'jp')
    ) THEN a.title -> 1 ->> 'en'
    ELSE a.title -> 2 ->> 'jp'
  END AS title,
  CASE
    WHEN LENGTH(a.title -> 0 ->> 'original') = GREATEST(
      LENGTH(a.title -> 0 ->> 'original'),
      LENGTH(a.title -> 1 ->> 'en'),
      LENGTH(a.title -> 2 ->> 'jp')
    ) THEN 'original'
    WHEN LENGTH(a.title -> 1 ->> 'en') = GREATEST(
      LENGTH(a.title -> 0 ->> 'original'),
      LENGTH(a.title -> 1 ->> 'en'),
      LENGTH(a.title -> 2 ->> 'jp')
    ) THEN 'en'
    ELSE 'jp'
  END AS title_version,
  GREATEST(
    LENGTH(a.title -> 0 ->> 'original'),
    LENGTH(a.title -> 1 ->> 'en'),
    LENGTH(a.title -> 2 ->> 'jp')
  ) AS max_length
FROM
  public.anime a
ORDER BY
  max_length DESC
LIMIT
  10;


-- Top 10 longest manga title
SELECT
  m.id AS manga_id,
  CASE
    WHEN LENGTH(m.title -> 0 ->> 'original') = GREATEST(
      LENGTH(m.title -> 0 ->> 'original'),
      LENGTH(m.title -> 1 ->> 'en'),
      LENGTH(m.title -> 2 ->> 'jp')
    ) THEN m.title -> 0 ->> 'original'
    WHEN LENGTH(m.title -> 1 ->> 'en') = GREATEST(
      LENGTH(m.title -> 0 ->> 'original'),
      LENGTH(m.title -> 1 ->> 'en'),
      LENGTH(m.title -> 2 ->> 'jp')
    ) THEN m.title -> 1 ->> 'en'
    ELSE m.title -> 2 ->> 'jp'
  END AS title,
  CASE
    WHEN LENGTH(m.title -> 0 ->> 'original') = GREATEST(
      LENGTH(m.title -> 0 ->> 'original'),
      LENGTH(m.title -> 1 ->> 'en'),
      LENGTH(m.title -> 2 ->> 'jp')
    ) THEN 'original'
    WHEN LENGTH(m.title -> 1 ->> 'en') = GREATEST(
      LENGTH(m.title -> 0 ->> 'original'),
      LENGTH(m.title -> 1 ->> 'en'),
      LENGTH(m.title -> 2 ->> 'jp')
    ) THEN 'en'
    ELSE 'jp'
  END AS title_version,
  GREATEST(
    LENGTH(m.title -> 0 ->> 'original'),
    LENGTH(m.title -> 1 ->> 'en'),
    LENGTH(m.title -> 2 ->> 'jp')
  ) AS max_length
FROM
  public.manga m
ORDER BY
  max_length DESC
LIMIT
  10;

-- Top 10 Studios based on mean score and production volume
WITH
  -- Calculate the mean score and anime count for each studio
  studio_stats AS (
    SELECT
      s.id AS studio_id,
      s.name AS studio_name,
      COUNT(DISTINCT a.id) AS anime_count,
      AVG(stat.mean) AS average_mean,
      -- Calculate raw weighted score
      AVG(stat.mean) * COUNT(DISTINCT a.id) AS weighted_score
    FROM
      public.studio s
      JOIN public.anime_studio anime_studio ON s.id = anime_studio.studio_id
      JOIN public.anime a ON anime_studio.anime_id = a.id
      JOIN public.anime_statistics anime_statistics ON a.id = anime_statistics.anime_id
      JOIN public.statistics stat ON anime_statistics.statistics_id = stat.id
    WHERE
      stat.mean IS NOT NULL
    GROUP BY
      s.id,
      s.name
  ),
  -- Calculate max and min weighted scores for normalization
  normalization_stats AS (
    SELECT
      MAX(weighted_score) AS max_weighted_score,
      MIN(weighted_score) AS min_weighted_score
    FROM
      studio_stats
  )
SELECT
  studio_id,
  studio_name,
  anime_count,
  average_mean,
  -- Normalize weighted_score to range 1-10
  CASE
    WHEN max_weighted_score = min_weighted_score THEN 1
    ELSE (weighted_score - min_weighted_score) / (max_weighted_score - min_weighted_score) * 9 + 1
  END AS normalized_weighted_score
FROM
  studio_stats
  CROSS JOIN normalization_stats
ORDER BY
  normalized_weighted_score DESC
LIMIT
  10;


-- Percentage of naughty animes
WITH
  genre_lookup AS (
    SELECT
      id
    FROM
      public.genre
    WHERE
      name IN ('Hentai', 'Ecchi', 'Harem', 'Erotica', 'Magical Sex Shift', 'Reverse Harem')
  ),
  hentai_ecchi_anime AS (
    SELECT
      DISTINCT ag.anime_id
    FROM
      public.anime_genre ag
      JOIN genre_lookup gl ON ag.genre_id = gl.id
  ),
  total_anime_count AS (
    SELECT
      COUNT(*) AS total_anime
    FROM
      public.anime
  ),
  hentai_ecchi_count AS (
    SELECT
      COUNT(*) AS hentai_ecchi_anime
    FROM
      hentai_ecchi_anime
  )
SELECT
  ROUND(
    (
      SELECT
        hentai_ecchi_anime
      FROM
        hentai_ecchi_count
    )::decimal / (
      SELECT
        total_anime
      FROM
        total_anime_count
    ) * 100,
    2
  ) AS hentai_ecchi_percentage;


-- Seasonal distribution
SELECT
    season,
    COUNT(*) AS anime_count
FROM
    public.anime
WHERE
    season IS NOT NULL
GROUP BY
    season
ORDER BY
    anime_count DESC;
