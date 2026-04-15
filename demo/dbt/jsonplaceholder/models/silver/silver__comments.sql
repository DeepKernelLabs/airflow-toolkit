SELECT
    id::INTEGER        AS id,
    "postId"::INTEGER  AS post_id,
    name::TEXT         AS name,
    email::TEXT        AS email,
    body::TEXT         AS body,
    _ds,
    _loaded_at,
    _loaded_from
FROM {{ ref('bronze__comments') }}
