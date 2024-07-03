import os
from dotenv import load_dotenv
from csv_reader import CsvReader
from snowflake_connector import SnowflakeUploader

class MainLoader_Bronze:
    def __init__(self) -> None:
        load_dotenv()
        self.dir_main_path      = os.getenv('main_route')
        self.dir_csv_path       = os.getenv('csv_main_route')
        self.dir_base_vtas      = self.dir_csv_path+'BASE_VTAS.csv'
        self.dir_mstr_cust      = self.dir_csv_path+'MSTR_CUSTOMER.csv'
        self.dir_mstr_itms      = self.dir_csv_path+'MSTR_ITEMS.csv'
        self.dir_mstr_pais      = self.dir_csv_path+'MSTR_PAIS.csv'
        self.dir_mstr_prio      = self.dir_csv_path+'MSTR_PRIORIDAD.csv'
        self.dir_base_vtax      = self.dir_csv_path+'BASE_VTAS_XL.csv'
        self.dir_mstr_fech      = self.dir_csv_path+'MSTR_FECHAS.csv'
        #Snowflake
        self.snow_conn = SnowflakeUploader(user=os.getenv('snow_user'),password=os.getenv('snow_password'),
                                      account=os.getenv('snow_account'), warehouse=os.getenv('snow_warehouse'),
                                      database=os.getenv('snow_database'), schema=os.getenv('snow_schema'))
        #self.snow_conn.test_connection()
        #BatchReader
        self.csv_reader_class   = csv_read = CsvReader()

    def read_data_batch(self):
        self.data_base_vtas = self.csv_reader_class.read_csv(file_path=self.dir_base_vtas)
        self.data_mstr_cust = self.csv_reader_class.read_csv(file_path=self.dir_mstr_cust)
        self.data_mstr_itms = self.csv_reader_class.read_csv(file_path=self.dir_mstr_itms)
        self.data_mstr_pais = self.csv_reader_class.read_csv(file_path=self.dir_mstr_pais)
        self.data_mstr_prio = self.csv_reader_class.read_csv(file_path=self.dir_mstr_prio)
        self.data_mstr_fech = self.csv_reader_class.read_csv(file_path=self.dir_mstr_fech)
        # New Data: BASE_VTAS_XL.csv
        self.data_base_vtax = self.csv_reader_class.read_csv(file_path=self.dir_base_vtax)
    def run(self) -> None:
        self.read_data_batch()
        #print(self.data_mstr_fech)
        #print(self.data_mstr_cust)
        #print(self.data_mstr_itms)
        #print(self.data_mstr_pais)
        #print(self.data_mstr_prio)
        print(self.data_base_vtax)
        #self.snow_conn.upload_to_snowflake(table_name='mstr_prio', data=self.data_mstr_prio)
        #self.snow_conn.upload_to_snowflake(table_name='mstr_pais', data=self.data_mstr_pais)
        #self.snow_conn.upload_to_snowflake(table_name='mstr_itms', data=self.data_mstr_itms)
        #self.snow_conn.upload_to_snowflake(table_name='mstr_cust', data=self.data_mstr_cust)
        #self.snow_conn.upload_to_snowflake(table_name='base_vtas', data=self.data_base_vtas)
        self.snow_conn.upload_to_snowflake(table_name='base_vtas_xl', data=self.data_base_vtax)
        #self.snow_conn.upload_to_snowflake(table_name='mstr_fech', data=self.data_mstr_fech)
        #--------------------------------
        self.snow_conn.close_connection()

runner = MainLoader_Bronze()
runner.run()
