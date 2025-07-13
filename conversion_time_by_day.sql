-- Step 1: Get first session and first purchase timestamps per user and day
WITH user_first_touch AS (
  SELECT
    FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP_MICROS(event_timestamp)) AS day,
    user_pseudo_id,
    MIN(IF(event_name = 'session_start', TIMESTAMP_MICROS(event_timestamp), NULL)) AS first_session,
    MIN(IF(event_name = 'purchase', TIMESTAMP_MICROS(event_timestamp), NULL)) AS first_purchase
  FROM bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131
  GROUP BY day, user_pseudo_id
),
-- Step 2: Calculate time to convert (in minutes) per user and day
conversion_time AS (
  SELECT
    day,
    TIMESTAMP_DIFF(first_purchase, first_session, MINUTE) AS time_to_convert_minutes
  FROM user_first_touch
  WHERE first_session IS NOT NULL AND first_purchase IS NOT NULL
),
-- Step 3: Compute average conversion time per day
avg_conversion_time AS (
  SELECT
    day,
    ROUND(AVG(time_to_convert_minutes), 2) AS avg_time_to_convert
  FROM conversion_time
  GROUP BY day
),

-- Step 4: Filter only the selected date range for contextual analysis
filtered_days AS (
  SELECT *
  FROM avg_conversion_time
  WHERE day BETWEEN '2021-01-31' AND '2021-01-31'
),
-- Step 5: Add window functions to classify each day vs. the range and overall dataset
time_avg_table AS (
  SELECT
    day,
    avg_time_to_convert,
    AVG(avg_time_to_convert) OVER() AS time_selected_avg,
    MAX(avg_time_to_convert) OVER() AS max_selected,
    MIN(avg_time_to_convert) OVER() AS min_selected
  FROM filtered_days
)

-- Final output: show daily average time to convert, segmentation vs. range and total
SELECT 
  day,
  avg_time_to_convert,
  CASE
    WHEN avg_time_to_convert = max_selected THEN 'max'
    WHEN avg_time_to_convert = min_selected THEN 'min'
    ELSE 'not_max_or_min'
  END AS max_min_segmentation,
  time_selected_avg,
  (SELECT AVG(avg_time_to_convert) FROM avg_conversion_time) AS total_avg,
  CASE
    WHEN time_selected_avg > (SELECT AVG(avg_time_to_convert) FROM avg_conversion_time) THEN 'Above avg'
    WHEN time_selected_avg < (SELECT AVG(avg_time_to_convert) FROM avg_conversion_time) THEN 'Below avg'
    ELSE 'Equal'
  END AS avg_segmentation
FROM time_avg_table
ORDER BY day
