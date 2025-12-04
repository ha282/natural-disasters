# Natural Disasters ETL
![](image.jpg)
## Project Objective

The goal of this project was to build an **ETL pipeline** that extracts, transforms and loads natural disaster data into a **SQL database**.  
The data is then used to create visualisations with **Streamlit**.

## ETL Pipeline

- **Extract**  
  Collects natural disaster data from a SQL database.

- **Transform**  
  Cleans, standardises, and enriches the data.  

- **Load**  
  Loads the processed data into a final SQL table for analysis.

## Visualisation

- Uses Streamlit to visualise clean data loaded into SQL Pagilla database


## Data Source
- Data was obtained from EM-DAT: https://www.emdat.be/



## Project Plan

## EPIC 1

```text

As a Data Analyst/Scientist,
I want to be able to access the natural disaster data from the SQL database,
so that it can be transformed ready for analysis
```

---
---

## EPIC 2

```text
As a Data Analyst/Scientist,
I want to be able to access cleaned, standardised natural disaster data,
so that I can easily analyse it and generate meaningful insights.
```

---
---

## EPIC 3

```text
As a Data Analyst/Scientist,
I want to be able to access the expanded and cleaned dataset in an SQL table,
so that I can use this data in analysis
```

---
---

## EPIC 4

```text
As an End User,
I want to interact with insightful visualisations of disaster-related data,
So that I can explore insights easily.

```

---
---

## Kanban
Link to Kanban board https://github.com/users/ha282/projects/3/views/1

## Data Quality
Excel (CSV) -> SQL
| ID of SQL or Checked Rows | Description of Check                                          | Result in SOURCE                                              | Result in DESTINATION                                         | OUTCOME |
|---------------------------|---------------------------------------------------------------|----------------------------------------------------------------|----------------------------------------------------------------|---------|
| SQL-1                    | Count of rows                                                 | 10,436                                                         | 10,436                                                         | PASS    |
| SQL-2                    | Count of Distinct Rows                                        | 10,436                                                         | 10,436                                                         | PASS    |
| SQL-3                    | Count of Columns                                              | 46                                                             | 46                                                             | PASS    |
| SQL-4                    | Date format Check <br><br>Compare 5 randomly chosen values from Source, check date matches in Destination. | 2009-05-22<br>2024-06-07<br>2019-06-17<br>2004-10-05<br>2006-09-17 | 2009-05-22<br>2024-06-07<br>2019-06-17<br>2004-10-05<br>2006-09-17 | PASS    |


## Setup
1. Environment
    Create `.env.dev` file in the project root with:
    ```env
    # source database
    SOURCE_DB_NAME={}
    SOURCE_DB_USER={}
    SOURCE_DB_PASSWORD={}
    SOURCE_DB_HOST={}
    SOURCE_DB_PORT={}

    # target database
    TARGET_DB_NAME={}
    TARGET_DB_USER={}
    TARGET_DB_PASSWORD={}
    TARGET_DB_HOST={}
    TARGET_DB_PORT={}
    ```
2. Create virtual environment
   
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate   # macOS/Linux
   .\.venv\\Scripts\\activate  # Windows
   ```
3. Install required packages: 
   ```bash
   pip install -r requirements.txt
   ```

4. Run ETL \
To run the script:
    ```bash
    run_etl dev
    ```

The script will extract the natural disasters dataset from SQL, transform the data and load the clean data into the SQL Pigilla database

## Project Tree

  
```
config/
├─ __init__.py
├─ env_config.py
├─ db_config.py
data/
├─ processed/
│  ├─ cleaned_dataset9.csv
├─ raw/
│  ├─ uncleaned-dataset9.csv
notebooks/
│  ├─ transform_dataset9.ipynb
scripts/
│  ├─ __init__.py
│  ├─ run_etl.py
scr/
├─ extract/
│  ├─ __init__.py
│  ├─ extract_dataset9.py
│  ├─ extract_query.py
│  ├─ extract.py
├─ load/
│  ├─ __init__.py
│  ├─ create_clean_dataset9.py
│  ├─ load.py
├─ sql/
│  ├─ extract_dataset9.sql
├─ transform/
│  ├─ __init__.py
│  ├─ clean_dataset9.py
│  ├─ transform.py
utils/
│  ├─ __init__.py
│  ├─ db_utils.py
│  ├─ logging_utils.py
|  ├─ sql_utils.py
test/

```


# FAQs

#### 1. How would you go about optimising query execution and performance if the dataset continues to increase?
- **Database indexing**: 
  Using database indexes on frequently queried columns would significantly improve query performance
- **Views**: The use of views could simplify complex queries and cache results for faster access
- **Partitioning tables**: Partitioning table based on disaster types or subtypes could reduce the amount of data scanned during queries, improving efficiency.


#### 2. What error handling and logging have you included in your code and how this could be leveraged?
- **Error Handling**: Implemented try-except blocks around key ETL functions such as extract and transform, for error handling. This made debugging easier.
- **Logging**: Used `logger.info()` to monitor key points in the ETL process. Also used it to log errors which helped with debugging

#### 3. Are there any security or privacy issues that you need to consider and how would you mitigate them?
- **Privacy Issues**: The dataset is intended for educational use only and must not be published. Users are required to have read and write access to a database.

#### 4. How this project could be deployed or adapted into an automated cloud environment using the AWS services you have covered?
- **Database**: Replace the PostgreSQL database with an Amazon RDS(postgres) or an alternative AWS postgres database. 
- **ETL**: AWS Glue for ETL job.
- **Monitor**: AWS CloudWatch can be used to monitor the ETL process and log the errors.
