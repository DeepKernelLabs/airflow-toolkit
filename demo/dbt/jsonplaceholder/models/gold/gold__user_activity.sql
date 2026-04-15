-- One row per user summarising their activity across posts, comments, albums and todos.
SELECT
    u.id                                                        AS user_id,
    u.name,
    u.email,
    COUNT(DISTINCT p.id)                                        AS posts_count,
    COUNT(DISTINCT c.id)                                        AS comments_count,
    COUNT(DISTINCT a.id)                                        AS albums_count,
    COUNT(DISTINCT t.id)                                        AS todos_count,
    SUM(CASE WHEN t.completed THEN 1 ELSE 0 END)               AS todos_completed,
    ROUND(
        100.0 * SUM(CASE WHEN t.completed THEN 1 ELSE 0 END)
              / NULLIF(COUNT(DISTINCT t.id), 0),
        1
    )                                                           AS todos_completion_pct
FROM {{ ref('silver__users') }}    u
LEFT JOIN {{ ref('silver__posts') }}    p ON p.user_id = u.id
LEFT JOIN {{ ref('silver__comments') }} c ON c.post_id = p.id
LEFT JOIN {{ ref('silver__albums') }}   a ON a.user_id = u.id
LEFT JOIN {{ ref('silver__todos') }}    t ON t.user_id = u.id
GROUP BY u.id, u.name, u.email
