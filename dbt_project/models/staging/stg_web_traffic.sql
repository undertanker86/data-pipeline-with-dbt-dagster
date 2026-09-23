select
    date,
    sessions,
    unique_visitors,
    page_views,
    bounce_rate,
    avg_session_duration_sec,
    traffic_source
from {{ source('raw', 'raw_web_traffic') }}
