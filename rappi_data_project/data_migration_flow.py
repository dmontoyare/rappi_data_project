from sqlalchemy import create_engine, text
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
def csv_to_bronze(filename, table_name):
    engine = get_engine()
    
    # Read CSV
    df = pd.read_csv(f'rappi_data_project/landing/{filename}')
    
    # Create table if not exists and insert data
    df.to_sql(table_name, engine, if_exists='replace', index=False)

@task 
def validate_balances():
    engine = get_engine()

    # Create a view to identify unbalanced transactions
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE OR ALTER VIEW unbalanced_transactions AS
            SELECT transaction_id
            FROM journal_entries
            GROUP BY transaction_id
            HAVING SUM(amount) != 0
        """))

@task
def transform_bronze_to_silver_view():
    engine = get_engine()
    with engine.begin() as conn:
        # Crear o modificar la vista
        conn.execute(text("""
            CREATE OR ALTER VIEW transformed_records AS
                SELECT
                    je.transaction_id,
                    je.transaction_date,
                    je.account_number,
                    acc.account_name,
                    CASE WHEN je.amount > 0 THEN je.amount ELSE 0 END AS debit_amount,
                    CASE WHEN je.amount < 0 THEN -je.amount ELSE 0 END AS credit_amount,
                    CASE 
                        WHEN YEAR(je.transaction_date) = 2024 
                            AND acc.account_number IS NOT NULL
                            AND NOT EXISTS (
                                SELECT 1 
                                FROM unbalanced_transactions ut 
                                WHERE ut.transaction_id = je.transaction_id
                            )
                        THEN CAST(1 AS BIT)
                        ELSE CAST(0 AS BIT)
                    END AS is_valid_transaction
                FROM journal_entries je
                LEFT JOIN accounts acc ON je.account_number = acc.account_number
        """))

# validate the percentage of invalid transactions
        result = conn.execute(text("""
            SELECT 
                COUNT(*) AS total,
                SUM(CASE WHEN is_valid_transaction = 0 THEN 1 ELSE 0 END) AS invalid
            FROM transformed_records
        """)).fetchone()

        total = result.total
        invalid = result.invalid
        invalid_ratio = invalid / total if total > 0 else 0

        if invalid_ratio > 0.05:
            raise ValueError(f"Error: el {invalid_ratio:.2%} de las transacciones son inválidas, lo cual supera el límite permitido del 5%.")

        

@task
def account_summary():
    engine = get_engine()

    # Create a view to calculate account balances
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE OR ALTER VIEW account_summary AS
            SELECT
                account_name,
                SUM(debit_amount) - SUM(credit_amount) AS final_balance
            FROM transformed_records
            WHERE is_valid_transaction = 1
            GROUP BY account_name
        """))



