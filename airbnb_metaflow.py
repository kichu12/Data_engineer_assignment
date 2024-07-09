from metaflow import FlowSpec, step, Parameter
import pandas as pd
from sqlalchemy import create_engine

# PostgreSQL connection details
DB_USERNAME = 'postgres'
DB_PASSWORD = 'Kichu123*#'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'airbnb_nyc'

class AirbnbETLMetaflowFlow(FlowSpec):

    @step
    def start(self):
        print("Starting the ETL process...")
        self.next(self.extract_data)

    @step
    def extract_data(self):
        print("Extracting data from PostgreSQL...")
        # Connect to PostgreSQL
        engine = create_engine(f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        
        # Query to extract data
        query = "SELECT * FROM airbnb_data;"
        
        # Execute query and fetch data into Pandas DataFrame
        self.df = pd.read_sql_query(query, engine)
        self.next(self.transform_data)

    @step
    def transform_data(self):
        print("Transforming data...")
        # Normalize the data
        self.df['last_review'] = pd.to_datetime(self.df['last_review'], errors='coerce')
        self.df['review_year'] = self.df['last_review'].dt.year
        self.df['review_month'] = self.df['last_review'].dt.month
        self.df['review_day'] = self.df['last_review'].dt.day
        
        # Calculate additional metrics
        avg_price_per_neighborhood = self.df.groupby('neighbourhood')['price'].mean()
        avg_price_per_neighborhood = avg_price_per_neighborhood.reset_index()
        avg_price_per_neighborhood.rename(columns={'price': 'avg_price'}, inplace=True)
        
        # Handle missing values
        self.df['reviews_per_month'] = self.df['reviews_per_month'].fillna(self.df['reviews_per_month'].mean())
        
        self.next(self.load_data)

    @step
    def load_data(self):
        print("Loading transformed data into PostgreSQL...")
        # Connect to PostgreSQL
        engine = create_engine(f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        
        # Insert transformed data into PostgreSQL
        self.df.to_sql('transformed_airbnb_data', engine, if_exists='replace', index=False)
        
        self.next(self.end)

    @step
    def end(self):
        print("ETL process completed successfully!")

if __name__ == '__main__':
    AirbnbETLMetaflowFlow()
