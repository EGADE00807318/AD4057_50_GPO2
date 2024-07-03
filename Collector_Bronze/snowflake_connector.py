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
        print(' Snowflake::: Connection created!')
        return conn
    
    def test_connection(self):
        query = f"SELECT * FROM DB_BRONZE_COMPANY.SCH_COLLECTOR_SYSTEM.BASE_VTAS limit 10;"
        print(query)
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(result, columns=columns)
            print(df)

        finally:
            cursor.close()


    def upload_to_snowflake(self, table_name, data):
        '''if not hasattr(self, 'data'):
            raise ValueError("No data to upload. Please read a CSV file first.")'''
        
        columns = data.columns.tolist()
        columns_str = ", ".join(columns)

        # Create a cursor object
        cursor = self.connection.cursor()

        try:
            # Create the table if it doesn't exist
            create_table_query = f"""
            CREATE OR REPLACE TABLE {self.database}.{self.schema}.{table_name} ({", ".join([f"{col} STRING" for col in columns])});
            """
            print(create_table_query)
            cursor.execute(create_table_query)
            
            # Upload data to Snowflake
            for i, row in data.iterrows():
                values_str = ", ".join([f"'{str(value)}'" for value in row.tolist()])
                insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str});"
                print(insert_query)
                cursor.execute(insert_query)
            
            print(f"Data uploaded successfully to {table_name} in Snowflake.")

        finally:
            cursor.close()

    def close_connection(self):
        self.connection.close()
        print(' Snowflake::: Snowflake connection closed.')