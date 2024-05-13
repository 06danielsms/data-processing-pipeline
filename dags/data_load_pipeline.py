from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import uuid
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# Configuración de la conexión a la base de datos
def create_db_connection():
    return create_engine('postgresql://airflow:airflow@postgres/airflow')

# Función para obtener todos los archivos CSV en un directorio, excluyendo el de validación
def get_csv_files(directory):
    return sorted([os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.csv') and 'validation' not in f])


# Generar un identificador único de lote usando UUID
def generate_batch_uuid():
    return str(uuid.uuid4())


# Inicialización de las estadísticas
def init_stats():
    return {'count': 0, 'sum_price': 0, 'min_price': np.inf, 'max_price': -np.inf, 'avg_price': 0}


# Actualización de las estadísticas
def update_stats_buffer_memory(stats, new_data):
    stats['count'] += len(new_data)
    stats['sum_price'] += new_data['price'].sum()
    stats['min_price'] = min(stats['min_price'], new_data['price'].min())
    stats['max_price'] = max(stats['max_price'], new_data['price'].max())
    if stats['count'] > 0:
        stats['avg_price'] = stats['sum_price'] / stats['count']
    else:
        stats['avg_price'] = 0
    return stats


# Cargar datos, añadir columna de origen y actualizar estadísticas
def load_data_and_update_stats(file_name, engine, stats, batch_uuid):
    data = pd.read_csv(file_name)
    data['source_file'] = os.path.basename(file_name)
    data['batch_uuid'] = batch_uuid
    file_size = os.path.getsize(file_name)
    data['file_size'] = file_size
    data.to_sql('transaction_data', con=engine, if_exists='append', index=False)
    stats = update_stats_buffer_memory(stats, data)
    print(f"Loaded {file_name} with file size {file_size} bytes and batch hash {batch_uuid}, updated stats")
    return stats


# Consultar estadísticas de la base de datos al terminar de cargar datos
def query_stats_sql(engine):
    query = text("""
        SELECT COUNT(*) AS total_count,
               AVG(price) AS avg_price,
               MIN(price) AS min_price,
               MAX(price) AS max_price
        FROM transaction_data
    """)
    with engine.connect() as conn:
        result = conn.execute(query)
        stats = result.fetchone()
        return {
            'total_count': stats[0],
            'avg_price': float(stats[1]),
            'min_price': float(stats[2]),
            'max_price': float(stats[3])
        }


# Función principal para procesar los archivos y realizar comprobaciones
def process_files_and_validate(directory, validation_file):
    engine = create_db_connection()
    stats = init_stats()
    batch_uuid = generate_batch_uuid()
    csv_files = get_csv_files(directory)
    for file in csv_files:
        stats = load_data_and_update_stats(file, engine, stats, batch_uuid)
        print("Current in-memory stats:", stats)
    
    print("\nProcessing validation file...")
    validation_stats = load_data_and_update_stats(validation_file, engine, stats, batch_uuid)
    print("Validation file stats:", validation_stats)
    
    final_db_stats = query_stats_sql(engine)
    print("Final database stats after loading validation.csv:", final_db_stats)

    return stats, validation_stats, final_db_stats


default_args = {
    'owner': 'daniel',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Definición del DAG
dag = DAG(
    'process_and_load_data',
    default_args=default_args,
    description='A simple DAG to process and load data',
    schedule_interval=None,
)


# Asumiendo que hay una función que encapsula la carga de datos y actualización de estadísticas
process_files = PythonOperator(
    task_id='process_files_and_validate',
    python_callable=process_files_and_validate,
    op_kwargs={'directory': '/input-process', 'validation_file': '/input-process/validation.csv'},
    dag=dag,
)

process_files
