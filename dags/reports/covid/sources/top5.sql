CREATE VIEW {view_name} AS (
WITH daily_changes AS (
    SELECT
        date,
        cases - LAG(cases, 1, 0) OVER (ORDER BY date) AS new_cases,
        deaths - LAG(deaths, 1, 0) OVER (ORDER BY date) AS new_deaths
    FROM
        ecovis_test.covid_us
)
SELECT
    date,
    new_cases,
    new_deaths
FROM
    daily_changes
ORDER BY
    new_cases DESC, new_deaths DESC
LIMIT 5
);