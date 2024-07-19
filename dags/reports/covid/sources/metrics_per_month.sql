CREATE OR REPLACE VIEW {view_name} AS
WITH daily_changes AS (
    SELECT
        date,
        cases - LAG(cases, 1, 0) OVER (ORDER BY date) AS new_cases,
        deaths - LAG(deaths, 1, 0) OVER (ORDER BY date) AS new_deaths
    FROM
        ecovis_test.covid_us
),
monthly_totals AS (
    SELECT
        DATE_TRUNC('month', date)::date AS month,
        SUM(cases) AS total_cases,
        SUM(deaths) AS total_deaths
    FROM
        ecovis_test.covid_us
    GROUP BY
        DATE_TRUNC('month', date)
)
SELECT
    m.month,
    m.total_cases,
    m.total_deaths,
    AVG(d.new_cases) AS avg_daily_new_cases,
    AVG(d.new_deaths) AS avg_daily_new_deaths
FROM
    monthly_totals m
JOIN
    daily_changes d ON DATE_TRUNC('month', d.date) = m.month
GROUP BY
    m.month, m.total_cases, m.total_deaths
ORDER BY
    m.month;
