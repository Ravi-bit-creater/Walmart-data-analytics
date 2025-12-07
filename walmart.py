import pandas as pd
import pymysql
from sqlalchemy import create_engine

# 1. Load the CSV
df = pd.read_csv('Walmart.csv', encoding_errors='ignore')
print("\nInitial Shape:", df.shape)

# 2. View first 5 rows
print("\nFirst 5 rows before cleaning:\n", df.head())

# 3. Count duplicate rows
print("\nDuplicate rows:", df.duplicated().sum())

# 4. Remove duplicates
df.drop_duplicates(inplace=True)
print("\nDuplicate rows after removal:", df.duplicated().sum())

# 5. Count missing values
print("\nNull values before dropping:\n", df.isnull().sum())

# 6. Remove rows with missing values
df.dropna(inplace=True)
print("\nNull values after dropping:\n", df.isnull().sum())

# 7. Check dataset shape
print("\nShape after cleaning:", df.shape)

# 8. Show column data types
print("\nData types before conversion:\n", df.dtypes)

# 9. Convert unit_price column to float
df['unit_price'] = df['unit_price'].str.replace('$', '').astype(float)

# 10. Create new column "total"
df['total'] = df['unit_price'] * df['quantity']

# 11. Final preview
print("\nFirst 5 rows after processing:\n", df.head())

# 12. Show columns
print("\nColumns:\n", df.columns)

print("\nData Transformation Completed Successfully ✔")

# 13. Save the Clean data into the new csv file
df.to_csv('walmart_cleaned_data.csv', index=False)

# 14. MySQL Connection
username = "vscode"
password = "RaviLeela%402027"   # encoded @ symbol
database = "walmart_db"
host = "localhost"
port = 3306

engine_mysql = create_engine(f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}")

try:
    connection = engine_mysql.connect()
    print("MySQL Connected Successfully")
    connection.close()
except Exception as e:
    print("Connection Failed:", e)

df.to_sql(
    name="walmart",
    con=engine_mysql,
    if_exists='append',
    index=False
)

print("Data inserted into MySQL successfully")