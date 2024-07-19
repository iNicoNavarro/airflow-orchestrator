UPDATE {final_table} as f
SET
    deleted_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP
FROM
    {final_table} AS final
        LEFT JOIN
    {staging_table} AS staging
        ON final.date   = staging.date
WHERE
    f.date  = final.date
    AND (
        COALESCE(f.cases, -1) != COALESCE(staging.cases, -1)
        OR COALESCE(f.deaths, -1) != COALESCE(staging.deaths, 1)
    )
    AND final.deleted_at IS NULL
    AND f.deleted_at IS NULL;
