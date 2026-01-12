# 📊 Project Scenario

A multi-national firm has hired you as a **Data Engineer**. Your job is to access and process data as per requirements.

## Task Overview
- Compile the list of the **top 10 largest banks in the world** ranked by **market capitalization (in billion USD)**.
- Transform the data to include values in:
  - **USD**
  - **GBP**
  - **EUR**
  - **INR**
- Use the exchange rate information provided in a CSV file.

## Deliverables
- Save the processed information table **locally in CSV format**.
- Store the same table in a **SQLite database**.
- Ensure managers from different countries can query the database table to extract the list and view market capitalization in their own currency.

# 🛠️ Concepts and Libraries Used

This project demonstrates a full **ETL (Extract, Transform, Load)** pipeline using Python.  
Below are the key concepts and libraries applied:

## 🔍 Data Extraction
- **`requests`** → Fetch HTML content from websites.
- **`BeautifulSoup` (bs4)** → Parse and extract structured data from HTML tables.
- **`glob`** → Locate files matching patterns (e.g., CSV or XML files).
- **`xml.etree.ElementTree`** → Parse XML files for structured data extraction.
- **`csv`** → Read and write CSV files directly.

## 🔄 Data Transformation
- **`pandas`** → Create and manipulate DataFrames, clean data, and perform transformations.
- **`numpy`** → Handle numerical operations and conversions (e.g., currency exchange calculations).
- **`datetime`** → Manage and format timestamps for logging and data versioning.

## 💾 Data Loading
- **`sqlite3`** → Store processed data into a relational database (SQLite).
- **SQL concepts** → Create tables, insert records, and query data for managers in different countries.

## 📝 Logging and Monitoring
- **`logging`** → Track the ETL process, log successes, and capture errors for debugging.

---

### 📊 Workflow Summary
1. **Extract**: Scrape the top 10 largest banks by market capitalization from a website.  
2. **Transform**: Convert market cap values into multiple currencies (USD, GBP, EUR, INR) using exchange rates from a CSV file.  
3. **Load**: Save the transformed data both as a local CSV file and into a SQLite database.  
4. **Query**: Managers can query the database to view market capitalization in their preferred currency.  
5. **Monitor**: Log all operations for transparency and error tracking.

---

### 🚀 Key Concepts
- **ETL Pipeline** (Extract → Transform → Load)
- **Web Scraping** (requests + BeautifulSoup)
- **Data Wrangling** (pandas + numpy)
- **File Handling** (glob, csv, XML parsing)
- **Database Integration** (sqlite3)
- **Error Handling & Logging** (logging)

