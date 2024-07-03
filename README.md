# AD4057_50_GPO2

Proyecto Final - Grupo 2 

Ingesta de información - MULTISHOP

### Setup

Consideremos un proyecto nativo en python. Se recomienda mantener environments individuales.

- python >3.8
- pip

### Requerimientos

Considerar lo documentado en archivo *requirements.txt* dentro del proyecto.

```python
pip install snowflake-connector-python
pip install python-dotenv
pip install pandas
```

### Variables de entorno

Crear y configurar dentro del folder *Collector_Bronze* del proyecto un archivo de entorno *.env* con las siguientes rutas de las fuentes del negocio:

```python
#---------------------------------------------------------
snow_user       = 'user_snowflake'
snow_password   = 'Password'
snow_account    = 'ethnllb-no95637'
snow_warehouse  = 'COMPUTE_WH'
snow_database   = 'DB_BRONZE_COMPANY'
snow_schema     = 'SCH_COLLECTOR_SYSTEM'
#---------------------------------------------------------
main_route      = '/Ruta/SFTP/Fuentes/AD4057_50_GPO2'
csv_main_route  = '/Ruta/SFTP/Fuentes/AD4057_50_GPO2/DB_1/'
```

