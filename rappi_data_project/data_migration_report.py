from sqlalchemy import create_engine
import pandas as pd
from prefect import task

# Connection variables
DB_USER = 'rappi1'
DB_PASSWORD = 'Acceso+10'
DB_SERVER = 'rappi1.database.windows.net'
DB_NAME = 'rappi1'
DB_DRIVER = 'ODBC Driver 17 for SQL Server'

# Create SQLAlchemy engine
def get_engine():
    connection_string = f"mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}:1433/{DB_NAME}?driver={DB_DRIVER}"
    return create_engine(connection_string)

@task
def generate_report():
    engine = get_engine()
    # Query transformed_records and unbalanced_transactions views
    transformed_df = pd.read_sql("SELECT * FROM transformed_records", engine)
    unbalanced_df = pd.read_sql("SELECT * FROM unbalanced_transactions", engine)

    # Write results to a text file
    with open("migration_report.txt", "w", encoding="utf-8") as f:
        f.write("=== Transformed Records ===\n")
        f.write(transformed_df.to_string(index=False))
        f.write("\n\n=== Unbalanced Transactions ===\n")
        f.write(unbalanced_df.to_string(index=False))