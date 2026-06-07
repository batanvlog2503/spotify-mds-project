-- models/gold/dim_users.sql
select distinct
    user_id, country, device_type,
    subscription_type, age
from {{ ref('spotify_silver') }}