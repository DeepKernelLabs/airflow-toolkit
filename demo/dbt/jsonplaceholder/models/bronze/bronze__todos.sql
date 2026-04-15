WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY _loaded_at DESC
        ) AS rn
    FROM {{ source('raw', 'todos') }}
    WHERE _ds = (SELECT MAX(_ds) FROM {{ source('raw', 'todos') }})
)
SELECT
    id,
    "userId",
    title,
    completed,
    _ds,
    _loaded_at,
    _loaded_from
FROM deduped
WHERE rn = 1
