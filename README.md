# Netflix Movies & TV Shows Data Analysis - ELT Pipeline

<img width="1364" height="613" alt="image" src="https://github.com/user-attachments/assets/b2cbc2c4-8226-45fe-bd8d-ddf329902b70" />


A comprehensive ELT (Extract-Load-Transform) data engineering project that processes Netflix content data using Python for extraction and SQL Server for transformation and analysis.

## 🎯 Project Overview

This project demonstrates the **ELT approach** where raw data is extracted using Python, loaded directly into SQL Server, and all transformations, cleaning, and analysis are performed using advanced SQL techniques. This showcases modern data warehousing practices where the database handles heavy computational work.

## 🏗️ Architecture

```
Raw Data (CSV) → Python (Extract) → SQL Server (Load) → SQL (Transform & Analyze)
```

**Key Difference from ETL:**
- **ETL:** Extract → Transform (Python) → Load
- **ELT:** Extract → Load → Transform (SQL Server)

## 📁 Project Structure

```
netflix-elt-project/
├── netflix_data_extract.py     # Python script for data extraction and loading
├── netflix_raw.sql            # SQL script to create raw staging table
├── netflix_data_analysis.sql  # Complete transformation and analysis queries
├── README.md                  # Project documentation
└── netflix_titles.csv         # Dataset (download separately)
```

## 🛠️ Technologies Used

- **Python 3.x**
  - pandas
  - sqlalchemy
- **SQL Server** (Express or Standard)
- **ODBC Driver 17 for SQL Server**

## 📊 Dataset Information

- **Source:** Netflix Movies and TV Shows Dataset
- **Records:** 8,807 rows
- **Columns:** 12 attributes including show_id, title, director, cast, country, release_year, etc.
- **Data Issues Handled:** Duplicates, encoding problems, missing values, data type inconsistencies

## 🚀 Setup & Installation

### Prerequisites
1. **SQL Server** (Express/Standard) installed
2. **Python 3.x** with pip
3. **ODBC Driver 17 for SQL Server**

### Python Dependencies
```bash
pip install pandas sqlalchemy pyodbc
```

### Database Setup
1. Ensure SQL Server is running
2. Update connection string in `netflix_data_extract.py`:
```python
engine = sal.create_engine('mssql://YOUR_SERVER/YOUR_DATABASE?driver=ODBC+DRIVER+17+FOR+SQL+SERVER')
```

## 📝 Step-by-Step Execution

### Step 1: Create Raw Table Schema
```sql
-- Execute netflix_raw.sql
CREATE TABLE [dbo].[netflix_raw](
    [show_id] [varchar](10) PRIMARY KEY,
    [type] [varchar](10) NULL,
    [title] [nvarchar](200) NULL,
    [director] [varchar](250) NULL,
    [cast] [varchar](1000) NULL,
    [country] [varchar](150) NULL,
    [date_added] [varchar](20) NULL,
    [release_year] [int] NULL,
    [rating] [varchar](10) NULL,
    [duration] [varchar](10) NULL,
    [listed_in] [varchar](100) NULL,
    [description] [varchar](500) NULL
);
```

### Step 2: Extract and Load Data
```python
# Execute netflix_data_extract.py
import pandas as pd
import sqlalchemy as sal

# Read CSV data
df = pd.read_csv('netflix_titles.csv')

# Create database connection
engine = sal.create_engine('mssql://ANKIT\\SQLEXPRESS/master?driver=ODBC+DRIVER+17+FOR+SQL+SERVER')
conn = engine.connect()

# Load data to SQL Server
df.to_sql('netflix_raw', con=conn, index=False, if_exists='append')
conn.close()

# Data exploration
print(df.head())
print(df.isna().sum())
print(f"Max description length: {max(df.description.dropna().str.len())}")
```

### Step 3: Transform and Analyze Data
Execute the complete `netflix_data_analysis.sql` script which includes:

## 🔧 Data Transformations Performed

### 1. Data Quality & Cleaning
```sql
-- Remove duplicates using window functions
WITH cte AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY title, type ORDER BY show_id) as rn
    FROM netflix_raw
)
SELECT * FROM cte WHERE rn = 1;
```

### 2. Data Type Conversions
```sql
-- Convert date_added to proper DATE format
CAST(date_added AS DATE) as date_added

-- Handle misplaced duration values
CASE WHEN duration IS NULL THEN rating ELSE duration END as duration
```

### 3. Data Normalization
```sql
-- Create normalized genre table
SELECT show_id, TRIM(value) as genre
INTO netflix_genre
FROM netflix_raw
CROSS APPLY STRING_SPLIT(listed_in, ',');

-- Similar normalization for directors, cast, and countries
```

## 📈 Business Intelligence Queries

### Query 1: Directors with Both Movies and TV Shows
```sql
SELECT nd.director,
    COUNT(DISTINCT CASE WHEN n.type='Movie' THEN n.show_id END) as no_of_movies,
    COUNT(DISTINCT CASE WHEN n.type='TV Show' THEN n.show_id END) as no_of_tvshow
FROM netflix n
INNER JOIN netflix_directors nd ON n.show_id = nd.show_id
GROUP BY nd.director
HAVING COUNT(DISTINCT n.type) > 1;
```

### Query 2: Country with Most Comedy Movies
```sql
SELECT TOP 1 nc.country, COUNT(DISTINCT ng.show_id) as no_of_movies
FROM netflix_genre ng
INNER JOIN netflix_country nc ON ng.show_id = nc.show_id
INNER JOIN netflix n ON ng.show_id = nc.show_id
WHERE ng.genre = 'Comedies' AND n.type = 'Movie'
GROUP BY nc.country
ORDER BY no_of_movies DESC;
```

### Query 3: Top Director by Year
```sql
WITH cte AS (
    SELECT nd.director, YEAR(date_added) as date_year, 
           COUNT(n.show_id) as no_of_movies
    FROM netflix n
    INNER JOIN netflix_directors nd ON n.show_id = nd.show_id
    WHERE type = 'Movie'
    GROUP BY nd.director, YEAR(date_added)
),
cte2 AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY date_year ORDER BY no_of_movies DESC, director) as rn
    FROM cte
)
SELECT * FROM cte2 WHERE rn = 1;
```

### Query 4: Average Duration by Genre
```sql
SELECT ng.genre, AVG(CAST(REPLACE(duration, ' min', '') AS INT)) as avg_duration
FROM netflix n
INNER JOIN netflix_genre ng ON n.show_id = ng.show_id
WHERE type = 'Movie'
GROUP BY ng.genre;
```

### Query 5: Directors with Horror and Comedy Movies
```sql
SELECT nd.director,
    COUNT(DISTINCT CASE WHEN ng.genre='Comedies' THEN n.show_id END) as no_of_comedy,
    COUNT(DISTINCT CASE WHEN ng.genre='Horror Movies' THEN n.show_id END) as no_of_horror
FROM netflix n
INNER JOIN netflix_genre ng ON n.show_id = ng.show_id
INNER JOIN netflix_directors nd ON n.show_id = nd.show_id
WHERE type = 'Movie' AND ng.genre IN ('Comedies', 'Horror Movies')
GROUP BY nd.director
HAVING COUNT(DISTINCT ng.genre) = 2;
```

## 💡 Advanced SQL Techniques Demonstrated

- **Window Functions:** `ROW_NUMBER()`, `RANK()`, `PARTITION BY`
- **Common Table Expressions (CTEs):** Multi-step transformations
- **String Functions:** `STRING_SPLIT`, `TRIM`, `REPLACE`
- **Conditional Aggregation:** `COUNT(CASE WHEN...)`
- **Advanced Joins:** Multiple table joins with complex conditions
- **Data Normalization:** Converting comma-separated values to normalized tables
- **Date Functions:** `YEAR()`, `CAST()` for date manipulations

## 📋 Key Insights Generated

1. **Content Distribution:** Analysis of movies vs TV shows across different dimensions
2. **Director Analytics:** Identification of versatile directors working across content types
3. **Geographic Trends:** Country-wise content production patterns
4. **Temporal Analysis:** Year-over-year content addition trends
5. **Genre Analytics:** Duration patterns and cross-genre analysis

## 🎯 Business Applications

- **Content Strategy:** Identifying successful director-genre combinations
- **Market Analysis:** Understanding geographic content preferences
- **Investment Decisions:** Analyzing content duration trends by genre
- **Partnership Opportunities:** Finding directors with diverse portfolios

## 🚀 Future Enhancements

- Add data visualization using Power BI or Tableau
- Implement automated data refresh pipelines
- Create stored procedures for recurring analyses
- Add data quality monitoring and alerting
- Implement incremental data loading strategies

## 📚 Learning Outcomes

This project demonstrates:
- **Modern ELT Architecture** implementation
- **Advanced SQL Skills** for data transformation
- **Database Design** and normalization principles
- **Data Quality** handling techniques
- **Business Intelligence** query development

## 🤝 Contributing

Feel free to fork this project and submit pull requests for improvements or additional analyses.

## 📄 License

This project is open source and available under the [MIT License](LICENSE).

---
