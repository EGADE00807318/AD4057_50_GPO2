#pip install pandas snowflake-connector-python
#pip install python-dotenv
#
import pandas as pd
from snowflake.connector import connect

class SnowflakeUploader:
    def __init__(self, user, password, account, warehouse, database, schema):
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.connection = self._create_connection()

    def _create_connection(self):
        conn = connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema
        )
        return conn

    def upload_to_snowflake(self, table_name):
        if not hasattr(self, 'data'):
            raise ValueError("No data to upload. Please read a CSV file first.")
        
        columns = self.data.columns.tolist()
        columns_str = ", ".join(columns)

        # Create a cursor object
        cursor = self.connection.cursor()

        try:
            # Create the table if it doesn't exist
            '''create_table_query = f"""
            CREATE OR REPLACE TABLE {table_name} ({", ".join([f"{col} STRING" for col in columns])});
            """
            cursor.execute(create_table_query)
            '''
            # Upload data to Snowflake
            for i, row in self.data.iterrows():
                values_str = ", ".join([f"'{str(value)}'" for value in row.tolist()])
                insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str});"
                cursor.execute(insert_query)
            
            print(f"Data uploaded successfully to {table_name} in Snowflake.")

        finally:
            cursor.close()

    def close_connection(self):
        self.connection.close()

# Uso de la clase
'''
if __name__ == "__main__":
    # Configura tus credenciales de Snowflake
    user = 'TU_USUARIO'
    password = 'TU_CONTRASEÑA'
    account = 'TU_CUENTA'
    warehouse = 'TU_WAREHOUSE'
    database = 'TU_DATABASE'
    schema = 'TU_SCHEMA'

    # Inicializa la clase
    uploader = SnowflakeUploader(user, password, account, warehouse, database, schema)

    # Lee el archivo CSV
    csv_file_path = 'ruta/a/tu/archivo.csv'
    uploader.read_csv(csv_file_path)

    # Sube los datos a Snowflake
    table_name = 'nombre_de_tu_tabla'
    uploader.upload_to_snowflake(table_name)

    # Cierra la conexión
    uploader.close_connection()
    '''
