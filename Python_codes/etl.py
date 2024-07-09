import pandas as pd
from sqlalchemy import create_engine

# PostgreSQL connection details
DB_USERNAME = 'postgres'
DB_PASSWORD = 'Kichu123*#'
DB_HOST = 'localhost'  
DB_PORT = '5432'  
DB_NAME = 'airbnb_nyc'

def extract_data():
    # Connect to PostgreSQL
    engine = create_engine(f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    
    # Query to extract data
    query = "SELECT * FROM airbnb_data;"
    
    # Execute query and fetch data into Pandas DataFrame
    df = pd.read_sql_query(query, engine)
    
    return df

def transform_data(df):
    # Convert 'last_review' to datetime format
    df['last_review'] = pd.to_datetime(df['last_review'], errors='coerce')
    df['review_year'] = df['last_review'].dt.year
    df['review_month'] = df['last_review'].dt.month
    df['review_day'] = df['last_review'].dt.day
    
    # Calculate additional metrics
    # Example: Average price per neighborhood
    avg_price_per_neighborhood = df.groupby('neighbourhood')['price'].mean()
    avg_price_per_neighborhood = avg_price_per_neighborhood.reset_index()
    avg_price_per_neighborhood.rename(columns={'price': 'avg_price'}, inplace=True)
    
    # Handle missing values
    # Fill missing values in 'reviews_per_month' with mean
    df['reviews_per_month'] = df['reviews_per_month'].fillna(df['reviews_per_month'].mean())
    
    return df, avg_price_per_neighborhood

def load_transformed_data(transformed_data, table_name):
    # Connect to PostgreSQL
    engine = create_engine(f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    
    # Insert transformed data into PostgreSQL
    transformed_data.to_sql(table_name, engine, if_exists='replace', index=False)
    
    print(f"Transformed data loaded into table '{table_name}' successfully!")

if __name__ == "__main__":
    # Step 1: Extract data
    data = extract_data()
    print("Data extracted successfully!")
    
    # Step 2: Transform data
    transformed_data, avg_price_per_neighborhood = transform_data(data)
    print("Data transformed successfully!")
    
    # Print transformed data
    print("\nTransformed Data:")
    print(transformed_data.head())
    
    # Print average price per neighborhood
    print("\nAverage Price per Neighborhood:")
    print(avg_price_per_neighborhood.head())
    
    # Step 3: Load transformed data into PostgreSQL
    table_name = 'transformed_airbnb_data'
    load_transformed_data(transformed_data, table_name)
