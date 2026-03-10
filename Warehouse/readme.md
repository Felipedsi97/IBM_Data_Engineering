# Simple DDL, Star modeling and data for importing

Data and structure to apply grouping sets, cube, rollup and other concepts of data warehousing

## Star Schema
- used to OLTP, good for analytics, bad for data insertion and update. Tends to have less joins, usually one, instead of multiple complex JOINS from normalized tables;
- normalization in data context means break down a table into small units, usually atomic units to minimize redundancy, integrity and efficiency (single source of truth SSOT);
- fact table needs to hold all the foreign keys (1:M);
- fact tables have measurements, values and quantitative information;
- dimension tables have attributes, context and qualitative information;
