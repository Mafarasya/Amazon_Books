# Directed Acyclic Graph
from datetime import datetime, timedelta
from airflow import DAG
import requests
import pandas as pd
from bs4 import BeautifulSoup
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import time

headers = {
    "Referer": 'https://www.amazon.com/',
    "Sec-Ch-Ua": "Not_A Brand",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "macOS",
    'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
}

# Tasks:
# 1) Fetch amazon books data (extract phase)
# 2) Data Cleansing (Transform)
def get_amazon_data_books(num_books, ti):
    # Base url of the amazon search results for data science books
    base_url = f"https://www.amazon.com/s?k=data+engineering+books"

    books = []
    seen_titles = set() # keep track of seen titles to prevent duplicates

    page = 1

    max_retries = 5  
    retry_count = 0

    while len(books) < num_books:
        url = f"{base_url}&page={page}"
        print(f"Fetching URL: {url}")

        # Send request to the URL
        # Check if the request was successful
        try:
            response = requests.get(url, headers=headers)
            print(f"Status code: {response.status_code}")
            if response.status_code == 503:
                print("Service unavailable (503). Retrying...")
                retry_count += 1
                time.sleep(30) 
                if retry_count >= max_retries:
                    print("Max retries reached. Exiting.")
                    break
                continue
            elif response.status_code != 200:
                print("Failed to retrieve the page.")
                break
        
            # Parse the content 
            soup = BeautifulSoup(response.content, "html.parser")

            # Find book containers
            book_containers = soup.find_all("div", {"class": "s-result-item"})

            # Loop through the book containers and extract the data
            for book in book_containers:
                title = book.find("span", {"class": "a-text-normal"})
                author = book.find("a", {"class": "a-size-base"})
                price = book.find("span", {"class": "a-price-whole"})
                rating = book.find("span", {"class": "a-icon-alt"})
                if title and author and price and rating:
                    book_title = title.text.strip()
                    print(title)

                    # Check if the tile has been seen before
                    if book_title not in seen_titles:
                        seen_titles.add(book_title)
                        books.append({
                            "Title": book_title,
                            "Author": author.text.strip(),
                            "Price": price.text.strip(),
                            "Rating": rating.text.strip()
                        })
            page += 1

            retry_count = 0  # Reset retry count after a successful request

        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            break
    # limit to the requested number of book
    books = books[:num_books]

    # Convert the list of dicts into DataFrame
    df = pd.DataFrame(books)
    print("Check dataframe")
    print(df.head())

    # Remove duplicates based on "Title" column
    df.drop_duplicates(subset="Title", inplace=True)

    # Push the DataFrame to XCom
    ti.xcom_push(key='book_data', value=df.to_dict('records'))


# 3) Create and store data in table on postgres (load)
def insert_book_data_into_postgres(ti):
    book_data = ti.xcom_pull(key='book_data', task_ids='fetch_book_data')
    if not book_data:
        raise ValueError("No book data found")

    postgres_hook = PostgresHook(postgres_conn_id='books_connection')
    insert_query = """
    INSERT INTO books (title, author, price, rating)
    VALUES (%s, %s, %s, %s)
    """
    for book in book_data:
        postgres_hook.run(insert_query, parameters=(book['Title'], book['Author'], book['Price'], book['Rating']))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'fetch_and_store_amazon_books',
    default_args = default_args,
    description= 'Simple DAG to fetch book data from amazon and store it in PostgreSQL',
    schedule_interval = timedelta(days=1)
)

# Operators:
fetch_book_data_task = PythonOperator(
    task_id = 'fetch_book_data',
    python_callable = get_amazon_data_books,
    op_args=[50], # Number of books to fetch
    dag=dag,
)

# Hooks - Postgres Hooks - Allow Connection:
create_table_task = PostgresOperator(
    task_id= 'create_table',
    postgres_conn_id = 'books_connection',
    sql = """
    CREATE TABLE IF NOT EXISTS books (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        author TEXT,
        price TEXT,
        rating TEXT
    );
    """,
    dag=dag,
)

# insert book data
insert_book_data_task = PythonOperator(
    task_id='insert_book_data',
    python_callable= insert_book_data_into_postgres,
    dag=dag,
)

# Dependencies


fetch_book_data_task >> create_table_task >> insert_book_data_task



