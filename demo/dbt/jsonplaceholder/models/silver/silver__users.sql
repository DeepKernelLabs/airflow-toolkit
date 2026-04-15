SELECT
    id::INTEGER   AS id,
    name::TEXT    AS name,
    username::TEXT AS username,
    email::TEXT   AS email,
    phone::TEXT   AS phone,
    website::TEXT AS website,
    _ds,
    _loaded_at,
    _loaded_from
FROM {{ ref('bronze__users') }}
