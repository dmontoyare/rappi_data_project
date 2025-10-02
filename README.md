# rappi_data_project


Para ejecutar este script:
•	Instalar dependencias.txt en el entorno virtual 
•	Ejecutar el script data_migration_dag.py
•	Crear una base de datos con las siguientes variables de coneccion (dicho metodo de coneccion no es seguro y solo se utiliza por simplicidad del ejemplo)
DB_USER = 'rappi1'
DB_PASSWORD = 'Acceso+10'
DB_SERVER = 'rappi1.database.windows.net'
DB_NAME = 'rappi1'
DB_DRIVER = 'ODBC Driver 17 for SQL Server'


Resultados obtenidos:
Se encontraron los siguientes registros inválidos para la migración	
-	2 no cumplieron con año == 2024
-	2 no cumplieron con account_number IN accounts
-	2 no cumplieron con balance == 0
el límite de 5% de transacciones invalidas no fue sobrepasado

Notas:
•	Para continuar con la lógica de manipular la DB desde el IDE se ha creado la función csv_to_bronze() cuya respectiva tarea ha sido agregada al DAG 
•	La función generate_report() ha sido agregada en un módulo a parte con el objetivo de cumplir con el principio Single Responsibility Principle (SRP), de esta forma el script data_migration_dag.py, solo se encarga de la orquestación y el script data_migration_flow.py se encarga únicamente en migración y transformación de datos