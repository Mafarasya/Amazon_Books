# Amazon Books Data Pipeline with Airflow and PostgreSQL

This project is an Airflow-based data pipeline that scrapes book information from Amazon, performs data transformation, and stores the data into a PostgreSQL database. This pipeline automates the ETL (Extract, Transform, Load) process using a Directed Acyclic Graph (DAG) with Airflow.

---

## Table of Contents
1. [Project Overview](#project-overview)
2. [Technologies Used](#technologies-used)
3. [Installation](#installation)
4. [Pipeline Structure](#pipeline-structure)
5. [DAG Configuration](#dag-configuration)
6. [Tasks](#tasks)
7. [Running the DAG](#running-the-dag)
8. [Screenshots](#screenshots)

---

## Project Overview
The pipeline follows the typical ETL process:
1. **Extract**: Retrieve book data from Amazon’s search page for “data engineering books”.
2. **Transform**: Clean and format the data.
3. **Load**: Insert the cleaned data into a PostgreSQL database.

---

## Technologies Used
- **Airflow**: Task scheduling and orchestration.
- **PostgreSQL**: Database for storing the extracted data.
- **Requests & BeautifulSoup**: For web scraping Amazon book data.
- **Docker**: To containerize the project (optional).
- **Pandas**: Data manipulation and cleaning.

---

## Installation
1. Clone the repository:
   ```bash
   git clone <repository-url>
