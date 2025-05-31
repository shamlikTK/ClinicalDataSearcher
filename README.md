This project is aimed to get data from https://clinicaltrials.gov/ and create transform and load to a PostgreSQL data bases in the *search optimized way* 

# Architecture diagram 

![image](https://github.com/shamlikTK/ClinicalDataSearcher/blob/main/data/sc/1.png)



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
 

# DataBase Schema




![[erd.png]]

####  Schema Optimization Notes

- The database schema is optimized primarily for search performance rather than strict normalization.
- It does not follow full 3NF to reduce the complexity of joins during search operations.
- Fields with high search frequency are denormalized and included directly in the main `clinical_trials` table to improve query speed.
- A separate `search_vectors` table is used to support full text search capabilities using data type `tsvector`



## Production  design 

The above design is mainly for demonstration purposes. If I were to build a production-ready data platform, I would choose the following pattern:



![[tracking_system2 2.png]]




This system supports both operational workflows and analytical workloads by integrating a **PostgreSQL OLTP database** for console operations and a **data warehouse** for business analytics.

#### Data Flow Architecture

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


