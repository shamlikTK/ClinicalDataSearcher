This project is aimed to get data from https://clinicaltrials.gov/ and create transform and load to a PostgreSQL data bases in the *search optimized way* 




# Architecture diagram 

![image](https://github.com/shamlikTK/ClinicalDataSearcher/blob/main/data/sc/1.png)


# How to run 

```
git clone https://github.com/shamlikTK/ClinicalDataSearcher
cd ClinicalDataSearcher
docker compose up -d
```


### ETL Process

The ETL process uses [**Prefect**](https://www.prefect.io/) as the scheduler, serving as an alternative to Airflow. A Prefect flow named **clinical-trials-etl** has been created, and it executes the job every 24 hours.

The Prefect UI can be accessed at `http://127.0.0.1:4200` after running the Docker Compose setup.

#### Tasks

The ETL process consists of two main tasks:

- **Download Data**  
  - This task is responsible for downloading data from an external source.  
  - The **requests** library is used for this task.

- **Process Data**  
  This task is divided into three sub-tasks:

  1. **Validate JSON Structure**  
     - Uses **Pydantic** to verify the structure of the JSON data.

  2. **Transform Data**  
     - Transforms the raw data into proper fields.  
     - For example, the `minimumAge` field may contain values like "years", which are converted into a proper integer format.

  3. **Load Data**  
     - Uses **SQLAlchemy** as the ORM to load the processed data into the database.


####  Scheduler Screenshot
![image](https://github.com/shamlikTK/ClinicalDataSearcher/blob/main/data/sc/prefect.png)


# DataBase Schema
![image](https://github.com/shamlikTK/ClinicalDataSearcher/blob/main/data/sc/erd.png)


####  Schema Optimization Notes

- The database schema is optimized primarily for search performance rather than strict normalization.
- It does not follow full 3NF to reduce the complexity of joins during search operations.
- Fields with high search frequency are denormalized and included directly in the main `clinical_trials` table to improve query speed.
- A separate `search_vectors` table is used to support full text search capabilities using data type `tsvector`


## Production  design 

The above design is mainly for demonstration purposes. If I were to build a production-ready data platform, I would choose the following pattern:



![tracking_system2](https://github.com/shamlikTK/ClinicalDataSearcher/blob/main/data/sc/2.png)




This system supports both operational workflows and analytical workloads by integrating a **PostgreSQL OLTP database** for console operations and a **data warehouse** for business analytics.

#### Data Flow Architecture and Multi stage 

1. **System of Records**
   - Contains raw files, including EMA and Clinical Trials formats.

2. **Transformation Layer**
   - Converts raw records into domain-specific data.
   - Output is typically a large flat table or JSON-like structure stored in a NoSQL database.
   - This **domain-specific model** serves as the backbone of the system.

3. **Data Synchronization**
   - Any updates to the domain-specific data are automatically propagated to:
     - The **Console Database** (PostgreSQL)
     - The **Data Warehouse**
   - The data warehouse maintains full historical data using [**SCD Type 2**](https://en.wikipedia.org/wiki/Slowly_changing_dimension) methodology.
   - The Console Database holds only the most recent  values.

4. **Data Science & AI/ML Integration**
   - The **Data Science Team** uses the Data Warehouse for model training and feature store management.
   - AI/ML inferences are directly written back into the **Domain-Specific Model**.
   - These updates are then synced across the Console application and other dependent systems.
   - ML evaluation pipeline will run as part of this process

5. **Business Intelligence (BI)**
   - BI dashboards connect directly to the Data Warehouse for reporting and analytics.

6. **Communication Layer**
   - All system components communicate via a **message bus** (e.g., Kafka), ensuring reliable and scalable data flow.
   
7. **Tech stack**
   - The technical stack included Spark for data transformation, Kafka for message passing, PostgreSQL, Milvus, and Elasticsearch for database search, and Power BI for business intelligence
   - I would use Hirarchical chinking method of this type data.
   - DiskANN for Embedded Indexing

![tracking3](https://github.com/shamlikTK/ClinicalDataSearcher/blob/main/data/sc/3.png)

#### Data updation process:
The data update process will occur in two steps. The domain-specific model will notify both the OLTP and OLAP layers simultaneously. In the OLTP layer, values will be replaced directly. In the embedding database, only the corresponding chunk will be updated using checksum matching. For the OLAP layer, the domain-specific model will be transformed into a STAR schema. Additionally, three extra columns start_date, end_date, and flag will be added to the dimension tables to maintain historical data.


#### Diffent type of input data 

As mentioned in the Production Design section, all data will first be transformed into a system of record using a standardized schema. The same database and ETL process will be used to process and store multiple types of records, with specific columns included to identify the structure of each record.



#### Fields to be Standardized

Several fields need to be standardized to ensure a consistent format

    - `minimumAge, (age)`: Should be standardized to a consistent unit (e.g., "13 years").

    - `date`: Dates may appear in different formats, such as yyyy-mm-dd or yyyy-mm, and should be normalized accordingly.

    - `phoneNumber`: Values can vary, e.g., "+201159523871" or "414-520-7097"; a standard international format (e.g., E.164) should be enforced.

    - `detailedDescription, briefSummary`: These text fields should be stored in a way that optimizes them for efficient search and retrieval.

#### Note:
I utilized a LLM to assist in building this project.I independently designed the system architecture and developed the schema.
