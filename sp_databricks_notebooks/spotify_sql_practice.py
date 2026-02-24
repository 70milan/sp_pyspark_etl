"""
SPOTIFY DATA ENGINEERING - SQL/PySpark PRACTICE
==============================================
6 Medium-Hard Problems + 6 Quick Skill Questions
All problems include test cases for validation

SCHEMA REFERENCE:
-----------------
tmp_tracks: track_id, track_name, date_added
tmp_track_meta: track_id, disc_number, duration_ms, explicit, is_playable, popularity, preview_url, track_number, type, uri
tmp_track_markets: track_id, market
tmp_external_ids: track_id, isrc
tmp_albums: track_id, album_id, album_name, album_release_date, album_art
tmp_album_markets: album_id, market
tmp_artists: track_id, artist_id, artist_name, artist_href, artist_spotify_url
tmp_genartists: artist_id, genres
tmp_sp_master: (user's liked songs with track features - contains: danceability, energy, key, loudness, mode, speechiness, acousticness, instrumentalness, liveness, valence, tempo, time_signature, track_id)
tmp_tracks_features: (1M+ tracks features - same schema as sp_master for features)
"""

# ============================================================================
# PROBLEM 1: Genre Popularity Trends Over Time (MEDIUM-HARD)
# ============================================================================
"""
PROBLEM: Calculate monthly genre popularity trends for your liked songs.
For each genre and month, calculate:
- Average track popularity
- Number of tracks added
- Cumulative tracks added (running total)
- Rank genres by popularity within each month

REQUIREMENTS:
1. Extract year-month from date_added
2. Join tracks -> artists -> genres -> track_meta for popularity
3. Use window functions for cumulative sum and ranking
4. Handle tracks with multiple genres (one row per track-genre)
5. Order by year_month, rank

EXPECTED OUTPUT COLUMNS:
year_month, genre, avg_popularity, tracks_added, cumulative_tracks, popularity_rank
"""

# YOUR SOLUTION (SQL):
query_1_sql = """

WITH cte AS (
    SELECT 
        ag.genre,
        CONCAT(YEAR(t.date_added), '/', MONTH(t.date_added)) AS yearmonth,
        ROUND(AVG(m.popularity), 2) AS avg_pop,
        COUNT(t.track_id) AS total_tracks,
        ROUND(SUM(m.duration_ms / 1000 / 60), 2) AS tot_duration_mins
    FROM spotify_pipeline.silver.user_tracks t
    JOIN spotify_pipeline.silver.user_artists a ON a.track_id = t.track_id
    JOIN spotify_pipeline.silver.user_artist_genre ag ON ag.artist_id = a.artist_id
    JOIN spotify_pipeline.silver.user_track_meta m ON m.track_id = t.track_id
    WHERE ag.genre IS NOT NULL
    GROUP BY ag.genre, yearmonth
)
SELECT 
    genre,
    yearmonth,
    avg_pop,
    total_tracks,
    SUM(total_tracks) OVER (PARTITION BY genre ORDER BY yearmonth) AS cumulative_tracks,
    DENSE_RANK() OVER (PARTITION BY yearmonth ORDER BY avg_pop DESC) AS rank
FROM cte
ORDER BY rank

"""

# YOUR SOLUTION (PySpark):
def solution_1_pyspark():
    """
    Implement using PySpark DataFrame API
    Hint: Use date_format(), groupBy(), window functions (rank, sum)
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    
    # Your code here
    result = spark.sql("SELECT 1 as placeholder")
    return result

# TEST CASES for Problem 1:
# - Should have one row per (year_month, genre) combination
# - popularity_rank should restart at 1 for each year_month
# - cumulative_tracks should be monotonically increasing per genre
# - All tracks_added should be positive integers


# ============================================================================
# PROBLEM 2: Audio Feature Similarity Clustering (HARD)
# ============================================================================
"""
PROBLEM: Find clusters of similar tracks based on audio features.
For each track in your liked songs, find the top 3 most similar tracks from 
the super tracks features dataset using cosine similarity.

REQUIREMENTS:
1. Use audio features: danceability, energy, valence, tempo (normalized 0-1)
2. Calculate cosine similarity between your liked tracks and all other tracks
3. Exclude the track itself from results
4. Return top 3 matches per track with similarity score
5. Include track and artist names in output

FORMULA: Cosine similarity = dot_product(A,B) / (magnitude(A) * magnitude(B))

EXPECTED OUTPUT COLUMNS:
source_track_id, source_track_name, similar_track_id, similar_track_name, 
similar_artist_name, similarity_score, similarity_rank
"""

# YOUR SOLUTION (PySpark):
def solution_2_pyspark():
    """
    This is advanced! Use cross join + vector operations
    Hint: Normalize tempo to 0-1 range first (divide by max tempo ~250)
    Consider using broadcast for the smaller liked songs dataset
    """
    from pyspark.sql import functions as F
    
    # Your code here
    result = spark.sql("SELECT 1 as placeholder")
    return result

# TEST CASES for Problem 2:
# - Should have exactly 3 rows per source track (top 3 similar)
# - similarity_score should be between 0 and 1
# - similarity_rank should be 1, 2, 3 for each source_track_id
# - No track should match with itself

display(
    spark.sql("""
              SELECT 
              t.track_name,
              count(market) as total_market     
              FROM spotify_pipeline.silver.user_tracks t
              join spotify_pipeline.silver.user_track_markets m
              on m.track_id = t.track_id
              group by t.track_name 
              """)
    )
# ============================================================================
# PROBLEM 3: Market Availability Analysis with Rolling Windows (MEDIUM-HARD)
# ============================================================================
"""
PROBLEM: Analyze market availability patterns over time.
Calculate the 90-day rolling average of unique markets available for newly added tracks.

REQUIREMENTS:
1. For each track, count distinct markets available
2. Calculate 90-day rolling average of market count (based on date_added)
3. Identify tracks that have significantly fewer markets than the rolling avg
   (less than 70% of rolling avg)
4. Include album info and calculate what % of tracks in that album are "limited availability"

EXPECTED OUTPUT COLUMNS:
track_id, track_name, album_name, date_added, market_count, 
rolling_90day_avg_markets, is_limited_availability, album_limited_pct
"""

# YOUR SOLUTION (SQL):
query_3_sql = """

WITH cte AS (
    SELECT 
        t.track_id,
        t.track_name,
        COUNT(market) AS total_markets
    FROM spotify_pipeline.silver.user_tracks t
    JOIN spotify_pipeline.silver.user_track_markets m 
        ON m.track_id = t.track_id
    GROUP BY t.track_id, t.track_name
), 
cte2 AS (
    SELECT 
        cte.track_name,
        cte.track_id,
        a.album_name,
        cte.total_markets,
        DATE(t.date_added) AS date_added,
        ROUND(
            AVG(total_markets) OVER (
                ORDER BY CAST(t.date_added AS TIMESTAMP)
                RANGE BETWEEN INTERVAL 90 DAYS PRECEDING AND CURRENT ROW
            ), 2
        ) AS rolling_average,
        CASE 
            WHEN ROUND(
                total_markets / AVG(total_markets) OVER (
                    ORDER BY CAST(t.date_added AS TIMESTAMP)
                    RANGE BETWEEN INTERVAL 90 DAYS PRECEDING AND CURRENT ROW
                ) * 100, 2
            ) < 90 
            THEN 1 
            ELSE 0 
        END AS is_limited_availability
    FROM cte
    JOIN spotify_pipeline.silver.user_tracks t 
        ON t.track_id = cte.track_id
    JOIN spotify_pipeline.silver.user_albums a 
        ON a.track_id = t.track_id
)
SELECT 
    *,
    ROUND(
        (SUM(CASE WHEN is_limited_availability = 1 THEN 1 ELSE 0 END) 
        OVER (PARTITION BY album_name) * 100.0)
        / COUNT(track_id) OVER (PARTITION BY album_name), 
        2
    ) AS album_limited_pct
FROM cte2
ORDER BY date_added DESC, album_name ASC;

"""

# YOUR SOLUTION (PySpark):
def solution_3_pyspark():
    """
    Use window with rangeBetween for time-based rolling calculations
    Hint: Convert date to unix timestamp for range calculations
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    
    # Your code here
    result = spark.sql("SELECT 1 as placeholder")
    return result

# TEST CASES for Problem 3:
# - rolling_90day_avg_markets should never be NULL for tracks after first 90 days
# - is_limited_availability should be boolean
# - album_limited_pct should be between 0 and 100


# ============================================================================
# PROBLEM 4: Artist Diversity Score (HARD)
# ============================================================================
"""
PROBLEM: Calculate an "Artist Diversity Score" for each artist based on their tracks.
The diversity score considers:
- Genre diversity (number of unique genres)
- Audio feature variance (std deviation of danceability, energy, valence)
- Album spread (number of unique albums)
- Temporal spread (days between first and last track added)

REQUIREMENTS:
1. Combine all metrics into a normalized score (0-100)
2. Rank artists by diversity score
3. Only include artists with at least 5 tracks in your liked songs
4. Show the most "consistent" (low diversity) vs "experimental" (high diversity) artists

EXPECTED OUTPUT COLUMNS:
artist_id, artist_name, track_count, genre_count, avg_feature_variance,
album_count, days_span, diversity_score, diversity_rank, diversity_category
(category: 'Consistent', 'Moderate', 'Experimental')
"""

# YOUR SOLUTION (PySpark):
def solution_4_pyspark():
    """
    Complex aggregations + normalization + scoring
    Hint: Use stddev() for variance, datediff() for temporal spread
    Normalize each metric to 0-1, then average for final score
    """
    from pyspark.sql import functions as F
    
    # Your code here
    result = spark.sql("SELECT 1 as placeholder")
    return result

# TEST CASES for Problem 4:
# - diversity_score should be between 0 and 100
# - Only artists with track_count >= 5
# - diversity_category should be one of: Consistent, Moderate, Experimental


# ============================================================================
# PROBLEM 5: Gap Analysis - Missing Track Features (MEDIUM)
# ============================================================================
"""
PROBLEM: Identify tracks in your liked songs that DON'T have matching features
in the super tracks features dataset. Then, for tracks that DO match, compare
your listening preferences vs the general track features distribution.

REQUIREMENTS:
1. Find liked tracks without features (anti-join)
2. Calculate feature percentiles for your liked tracks vs all tracks
3. Identify which audio features you prefer above/below average
4. Report coverage statistics


EXPECTED OUTPUT:
Part A - Missing Features Summary:
  total_liked_tracks, tracks_with_features, tracks_missing_features, coverage_pct

Part B - Feature Preference Analysis:
  audio_feature, your_avg, overall_avg, your_percentile, preference_category
  (preference_category: 'Strong Preference', 'Slight Preference', 'Average', 'Below Average')


"""

# YOUR SOLUTION (SQL + PySpark):
query_5a_sql = """
-- Part A: Coverage statistics

   
With cte as (
select distinct t.track_id as tracks_without_features from spotify_pipeline.silver.user_tracks t
left anti join (
    SELECT DISTINCT t.track_id, track_name
    FROM spotify_pipeline.silver.user_tracks t
    JOIN spotify_pipeline.silver.user_master_feature tf on tf.track_id = t.track_id
    union
    select t.track_id , t.track_name
    from spotify_pipeline.silver.user_tracks t
    join spotify_pipeline.silver.super_tracks_features tff on tff.id = t.track_id) t2 on t2.track_id = t.track_id )
SELECT 
    COUNT(t.track_id) AS total_liked_tracks,
    COUNT(t.track_id) - COUNT(c.tracks_without_features) AS tracks_with_features,
    COUNT(c.tracks_without_features) AS tracks_missing_features,
    ROUND(
        (COUNT(t.track_id) - COUNT(c.tracks_without_features)) * 100.0 
        / COUNT(t.track_id), 
        2
    ) AS coverage_pct_having_features_in_liked_songs
FROM spotify_pipeline.silver.user_tracks t
LEFT JOIN cte c ON c.tracks_without_features = t.track_id;    

"""

query_5b_sql = """
-- Part B: Feature preference analysis
WITH CTE AS (
    SELECT DISTINCT T.track_id,danceability,energy,key,loudness,mode,speechiness,acousticness,instrumentalness,liveness,valence,tempo
    FROM spotify_pipeline.silver.user_tracks t
    JOIN spotify_pipeline.silver.user_master_feature tf on tf.track_id = t.track_id
    UNION
    SELECT DISTINCT T.track_id,danceability,energy,key,loudness,mode,speechiness,acousticness,instrumentalness,liveness,valence,tempo
    FROM spotify_pipeline.silver.user_tracks t
    JOIN spotify_pipeline.silver.super_tracks_features tff on tff.id = t.track_id
),
CTE2 AS (
    SELECT 
        ROUND(AVG(danceability), 2) AS DANCEABILITY,
        ROUND(AVG(energy), 2) AS energy,
        ROUND(AVG(key), 2) AS key,
        ROUND(AVG(loudness), 2) AS loudness,
        ROUND(AVG(mode), 2) AS mode,
        ROUND(AVG(speechiness), 2) AS speechiness,
        ROUND(AVG(acousticness), 2) AS acousticness,
        ROUND(AVG(instrumentalness), 2) AS instrumentalness,
        ROUND(AVG(liveness), 2) AS liveness,
        ROUND(AVG(valence), 2) AS valence,
        ROUND(AVG(tempo), 2) AS tempo
    FROM CTE c
), cte3 as (
-- Replace CROSS APPLY with LATERAL VIEW STACK (Databricks/Spark SQL)
SELECT b.AUDIO_FEATURE, b.MY_AVERAGE 
FROM cte2 a
LATERAL VIEW STACK(
    11,  -- Number of features (changed from 13 to 11 - you had duplicates!)
    'danceability', a.danceability,
    'energy', a.energy,
    'key', a.key,
    'loudness', a.loudness,
    'mode', a.mode,
    'speechiness', a.speechiness,
    'acousticness', a.acousticness,
    'instrumentalness', a.instrumentalness,
    'liveness', a.liveness,
    'valence', a.valence,
    'tempo', a.tempo
) b AS AUDIO_FEATURE, MY_AVERAGE)
select c.audio_feature, my_average, overall_average from (SELECT b.AUDIO_FEATURE, b.OVERALL_AVERAGE
FROM (
    SELECT 
        ROUND(AVG(danceability), 2) AS danceability,
        ROUND(AVG(energy), 2) AS energy,
        ROUND(AVG(key), 2) AS key,
        ROUND(AVG(loudness), 2) AS loudness,
        ROUND(AVG(mode), 2) AS mode,
        ROUND(AVG(speechiness), 2) AS speechiness,
        ROUND(AVG(acousticness), 2) AS acousticness,
        ROUND(AVG(instrumentalness), 2) AS instrumentalness,
        ROUND(AVG(liveness), 2) AS liveness,
        ROUND(AVG(valence), 2) AS valence,
        ROUND(AVG(tempo), 2) AS tempo
    FROM spotify_pipeline.silver.super_tracks_features
) t
LATERAL VIEW STACK(
    11,
    'danceability', t.danceability,
    'energy', t.energy,
    'key', t.key,
    'loudness', t.loudness,
    'mode', t.mode,
    'speechiness', t.speechiness,
    'acousticness', t.acousticness,
    'instrumentalness', t.instrumentalness,
    'liveness', t.liveness,
    'valence', t.valence,
    'tempo', t.tempo
) b AS AUDIO_FEATURE, OVERALL_AVERAGE) as t2
join cte3 c on c.audio_feature = t2.audio_feature


"""

# TEST CASES for Problem 5:
# - coverage_pct should be between 0 and 100
# - your_percentile should be between 0 and 100
# - Must analyze all numeric features: danceability, energy, valence, etc.


# ============================================================================
# PROBLEM 6: Collaborative Playlist Builder (HARD)
# ============================================================================
"""
PROBLEM: Build a "smart playlist" using SQL/PySpark that recommends tracks
from the super dataset based on your listening patterns.

ALGORITHM:

Calculate your personal average audio feature profile
(e.g., average danceability) from your liked songs dataset.

Find tracks in the super dataset that:
--Have audio feature values similar to your profile (e.g., within ±0.1)
--Are NOT already in your liked songs list.
--Compute a similarity score between each candidate track and your profile
(e.g., 1 - ABS(danceability_difference)).
--Rank the results by similarity score in descending order.
--Return the top 50 recommended tracks.

EXPECTED OUTPUT COLUMNS:
track_id, artist_name, danceability, similarity_score, recommendation_rank
"""

# YOUR SOLUTION (PySpark):
def solution_6_pyspark():
    """
    Multi-step process: profile building -> filtering -> similarity -> ranking
    Hint: Create a temp view for your feature profile first
    """
    from pyspark.sql import functions as F
    
    # Your code here
    result = spark.sql("SELECT 1 as placeholder")
    return result

# TEST CASES for Problem 6:
# - Should return exactly 50 tracks (or fewer if not enough matches)
# - similarity_to_profile should be >= 0.85
# - recommendation_rank should be 1 to 50
# - No duplicates in output


# ============================================================================
# QUICK SKILL TEST QUESTIONS (Answer in Comments)
# ============================================================================

"""
QUESTION 1: How would you find tracks that have the same popularity score 
within the same album?

YOUR ANSWER:
"""

"""
QUESTION 2: What's the difference between using WHERE vs HAVING when filtering
tracks by popularity?

YOUR ANSWER:
"""

"""
QUESTION 3: When would you use a window function vs a GROUP BY for calculating
running totals of tracks added per month?

YOUR ANSWER:
"""

"""
QUESTION 4: How would you optimize a query that joins tmp_tracks, tmp_artists,
and tmp_genartists if you're filtering by a specific artist_id?

YOUR ANSWER:
"""

"""
QUESTION 5: Write a query to find the top 3 longest tracks for each genre.
(Write the actual SQL/PySpark code below)

YOUR ANSWER:
"""

"""
QUESTION 6: Explain the difference between broadcast join vs shuffle hash join
in Spark. When would you use each for the Spotify dataset?

YOUR ANSWER:
"""


# ============================================================================
# TEST RUNNER (Run this after completing solutions)
# ============================================================================

def run_all_tests():
    """
    Validates all solutions against test cases
    """
    print("=" * 80)
    print("RUNNING TEST SUITE")
    print("=" * 80)
    
    problems = [
        ("Problem 1: Genre Trends", solution_1_pyspark),
        ("Problem 2: Track Similarity", solution_2_pyspark),
        ("Problem 3: Market Analysis", solution_3_pyspark),
        ("Problem 4: Artist Diversity", solution_4_pyspark),
        ("Problem 6: Playlist Builder", solution_6_pyspark),
    ]
    
    for name, func in problems:
        print(f"\n{'='*80}")
        print(f"Testing: {name}")
        print(f"{'='*80}")
        try:
            result = func()
            print(f"✓ Query executed successfully")
            print(f"  Rows returned: {result.count()}")
            print(f"  Schema: {result.columns}")
            result.show(5, truncate=False)
        except Exception as e:
            print(f"✗ Error: {str(e)}")
    
    print("\n" + "="*80)
    print("TEST SUITE COMPLETE")
    print("="*80)

# Uncomment to run tests:
# run_all_tests()


# ============================================================================
# BONUS: Create Gold Layer Tables for Visualization
# ============================================================================

"""
After solving the problems above, create these gold layer tables optimized
for visualization/analytics:

1. gold_genre_trends: Monthly genre metrics (from Problem 1)
2. gold_artist_profiles: Artist diversity and characteristics (from Problem 4)
3. gold_track_features_enriched: All tracks with complete features + metadata
4. gold_market_availability: Track-level market analysis (from Problem 3)
5. gold_recommendations: Recommended tracks (from Problem 6)

Save these as Delta tables with appropriate partitioning:
"""

def create_gold_tables():
    """
    Create gold layer tables for visualization
    """
    # Example structure:
    # solution_1_pyspark().write.format("delta").mode("overwrite") \
    #     .partitionBy("year_month") \
    #     .saveAsTable("gold.genre_trends")
    
    pass

# ============================================================================
# LEARNING RESOURCES
# ============================================================================

"""
KEY PYSPARK CONCEPTS FOR DATABRICKS CERT:

1. Window Functions:
   - rank(), dense_rank(), row_number()
   - lag(), lead()
   - sum().over(), avg().over()
   - rangeBetween(), rowsBetween()

2. Joins & Optimization:
   - broadcast() for small tables
   - repartition() before joins
   - Anti-joins with left_anti

3. Aggregations:
   - groupBy() with multiple aggs
   - pivot() for wide format
   - percentile_approx() for percentiles

4. Date/Time:
   - date_format(), month(), year()
   - datediff(), date_add()
   - to_date(), to_timestamp()

5. UDFs vs Built-in:
   - Prefer built-in functions
   - Use @pandas_udf for vectorized operations

6. Performance:
   - Cache/persist for reused DataFrames
   - Partition pruning with Delta
   - Broadcast hints for small tables
"""