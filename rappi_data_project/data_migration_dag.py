from prefect import flow, get_run_logger
from data_migration_flow import (
    csv_to_bronze,
    transform_bronze_to_silver_view,
    validate_balances,
    account_summary
)
from data_migration_report import generate_report
import traceback



@flow(name="Data Migration Pipeline")
def data_migration_dag():
    logger = get_run_logger()
    try:
        csv_to_bronze('journal_entries.csv', 'journal_entries')
        csv_to_bronze('accounts.csv', 'accounts')
        validate_balances()
        transform_bronze_to_silver_view()    
        account_summary()
        generate_report()
    except Exception as e:
        logger.error("❌ Error durante la ejecución del DAG:")
        logger.error(traceback.format_exc())



# Run the DAG locally
if __name__ == "__main__":
    data_migration_dag()
