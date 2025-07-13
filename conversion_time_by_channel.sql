-- Calculates the average time (in minutes) between the first session and first purchase,
-- grouped by acquisition channel.

-- Step 1: Get first session and first purchase per user and associate their channel
with user_first_touch as (
select
user_pseudo_id,
traffic_source.source as channel,
min(IF(event_name='session_start',TIMESTAMP_MICROS(event_timestamp),null)) as first_session,
min(IF(event_name='purchase',TIMESTAMP_MICROS(event_timestamp),null)) as first_purchase
from bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131
group by user_pseudo_id,traffic_source.source
)
-- Step 2: Calculate the time to convert (in minutes) per user
,conversion_time as(
select
channel,
TIMESTAMP_DIFF(first_purchase, first_session, MINUTE) AS time_to_convert_minutes
from user_first_touch
WHERE first_session IS NOT NULL AND first_purchase IS NOT NULL
)
-- Step 3: Aggregate by channel and calculate average conversion time
select
channel,
round(avg(time_to_convert_minutes),2) as avg_time_to_convert_minute
from conversion_time
group by channel
order by avg_time_to_convert_minute desc
