SELECT
    id::INTEGER       AS id,
    "userId"::INTEGER AS user_id,
    title::TEXT       AS title,
    _ds,
    _loaded_at,
    _loaded_from
FROM {{ ref('bronze__albums') }}
