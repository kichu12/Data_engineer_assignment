import pandas as pd
from sqlalchemy import create_engine

# PostgreSQL connection details
DB_USERNAME = 'postgres'
DB_PASSWORD = 'Kichu123*#'
DB_HOST = 'localhost'  
DB_PORT = '5432'  
DB_NAME = 'airbnb_nyc'

# File path to the dataset
DATASET_FILE = 'D:\Downloads\AB_NYC_2019.csv'

def load_data():
    # Load dataset into Pandas DataFrame
    df = pd.read_csv(DATASET_FILE)
    
    # Connect to PostgreSQL
    engine = create_engine(f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    
    # Insert data into PostgreSQL
    df.to_sql('airbnb_data', engine, if_exists='replace', index=False)
    
    print("Data loaded successfully!")

if __name__ == "__main__":
    load_data()
