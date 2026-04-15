-- Deduplicate posts: keep only the latest partition and one row per id.
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY _loaded_at DESC
        ) AS rn
    FROM {{ source('raw', 'posts') }}
    WHERE _ds = (SELECT MAX(_ds) FROM {{ source('raw', 'posts') }})
)
SELECT
    id,
    "userId",
    title,
    body,
    _ds,
    _loaded_at,
    _loaded_from
FROM deduped
WHERE rn = 1
