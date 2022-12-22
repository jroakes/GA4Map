WITH 

raw_ga_4 AS (
 
            SELECT
            * except(row)
            FROM (
            SELECT
                -- extracts date from source table
                parse_date('%Y%m%d',regexp_extract(_table_suffix,'[0-9]+')) as table_date,
                -- flag to indicate if source table is `events_intraday_`
                case when _table_suffix like '%intraday%' then true else false end as is_intraday,
                *,
                row_number() over (partition by user_pseudo_id, event_name, event_timestamp order by event_timestamp) as row
            FROM
                `{project_id}.{dataset_id}.events_*`
            WHERE PARSE_DATE('%Y%m%d', _table_suffix) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL {num_days} DAY) AND CURRENT_DATE()
                
                )
            WHERE
            row = 1
 
            ),
 
events AS (
    SELECT
        parse_date("%Y%m%d", event_date) event_date,
        event_name,
        timestamp_micros(event_timestamp) as event_timestamp,
        user_pseudo_id,
        timestamp_micros(user_first_touch_timestamp) as user_first_touch_timestamp,
        geo.region as geo_region,
        geo.city as geo_city,
        traffic_source.name as channel,
        max(if(params.key = 'ga_session_id', params.value.int_value, null)) ga_session_id,
        max(if(params.key = 'ga_session_number', params.value.int_value, null)) ga_session_number,
        cast(max(if(params.key = 'session_engaged', params.value.string_value, null)) as int64) session_engaged,
        max(if(params.key = 'page_location', params.value.string_value, null)) page_location,
        max(if(params.key = 'page_title', params.value.string_value, null)) page_title,
        FROM raw_ga_4,
        UNNEST(event_params) AS params
        WHERE event_name in  UNNEST(['page_view', 'session_start'])

        GROUP BY event_date, event_name, event_timestamp, user_pseudo_id, user_first_touch_timestamp, channel, geo_region, geo_city
        )
 
 
SELECT 
*
FROM events
ORDER BY event_timestamp ASC