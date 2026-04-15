WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY _loaded_at DESC
        ) AS rn
    FROM {{ source('raw', 'comments') }}
    WHERE _ds = (SELECT MAX(_ds) FROM {{ source('raw', 'comments') }})
)
SELECT
    id,
    "postId",
    name,
    email,
    body,
    _ds,
    _loaded_at,
    _loaded_from
FROM deduped
WHERE rn = 1
