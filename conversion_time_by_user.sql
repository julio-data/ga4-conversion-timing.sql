-- Calculates the time (in minutes) from first session_start to first purchase per user.
-- Also compares each user's conversion time against the global average and segments it.

-- Step 1: Get first session and first purchase per user
WITH user_first_touch AS (
  SELECT
    user_pseudo_id,
    MIN(IF(event_name = 'session_start', TIMESTAMP_MICROS(event_timestamp), NULL)) AS first_session,
    MIN(IF(event_name = 'purchase', TIMESTAMP_MICROS(event_timestamp), NULL)) AS first_purchase
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`
  GROUP BY user_pseudo_id
),

-- Step 2: Calculate time between first session and purchase (in minutes)
conversion_deltas AS (
  SELECT
    user_pseudo_id,
    first_session,
    first_purchase,
    TIMESTAMP_DIFF(first_purchase, first_session, MINUTE) AS time_to_convert_minutes
  FROM user_first_touch
  WHERE first_session IS NOT NULL AND first_purchase IS NOT NULL
),

-- Step 3: Add average conversion time for comparison
conversion_with_avg AS (
  SELECT
    user_pseudo_id,
    time_to_convert_minutes,
    AVG(time_to_convert_minutes) OVER () AS avg_time_to_convert
  FROM conversion_deltas
)

-- Final output: user-level conversion time and segmentation
SELECT
  user_pseudo_id,
  time_to_convert_minutes,
  ROUND(avg_time_to_convert, 2) AS avg_time_to_convert,
  CASE
    WHEN time_to_convert_minutes > avg_time_to_convert THEN 'Above avg'
    WHEN time_to_convert_minutes < avg_time_to_convert THEN 'Below avg'
    ELSE 'Equal'
  END AS conversion_time_segmentation
FROM conversion_with_avg
ORDER BY time_to_convert_minutes DESC
