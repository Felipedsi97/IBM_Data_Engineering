CREATE DATABASE Test1;
-- grouping sets
SELECT 
    countryid,
    categoryid,
    SUM(amount) AS totalsales
FROM public."FactSales"
GROUP BY GROUPING SETS (
    (countryid, categoryid),  -- country + category
    (countryid),              -- country only
    (categoryid),             -- category only
    ()                        -- grand total
);

-- rollup
SELECT 
    d.Year,
    c.country,
    SUM(f.amount) AS totalsales
FROM public."FactSales" f
JOIN public."DimDate" d ON f.dateid = d.dateid
JOIN public."DimCountry" c ON f.countryid = c.countryid
GROUP BY ROLLUP (d.Year, c.country);

-- cube
SELECT 
    d.Year,
    c.country,
    AVG(f.amount) AS avg_sales
FROM public."FactSales" f
JOIN public."DimDate" d ON f.dateid = d.dateid
JOIN public."DimCountry" c ON f.countryid = c.countryid
GROUP BY CUBE (d.Year, c.country);

-- mqt
CREATE MATERIALIZED VIEW public.total_sales_per_country AS
SELECT 
    c.country,
    SUM(f.amount) AS total_sales
FROM public."FactSales" f
JOIN public."DimCountry" c ON f.countryid = c.countryid
GROUP BY c.country;

REFRESH MATERIALIZED VIEW public.total_sales_per_country;
