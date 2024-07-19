INSERT INTO {final_table}
(
    date,
    cases,
    deaths
)
SELECT
    staging.date,
    staging.cases,
    staging.deaths
FROM
    {staging_table} as staging
LEFT JOIN
    {final_table} as final
    ON final.date = staging.date
    AND final.deleted_at IS NULL
WHERE
    final.date IS NULL;