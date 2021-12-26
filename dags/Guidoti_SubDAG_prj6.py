from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

def create_tables(parent_dag_id,
                  task_id,
                  redshift_conn_id,
                  create_tables,
                  *args, **kwargs):
                    
    dag = DAG(
        f"{parent_dag_id}.{task_id}",
        **kwargs)
    
    list_of_creates = list()
    
    # Create a start dummy task
    start_operator = DummyOperator(task_id='Begin_Subdag_execution',
                                   dag=dag)
    
    # Iterates over the provided dictionary containing the name of the tables
    # and the DDL to create them
    for key, value in create_tables.items():
    
        # Create a create_table task dinamically
        create_table = PostgresOperator(
            task_id="create_{}".format(key),
            dag=dag,
            postgres_conn_id="redshift",
            sql=value
        )
        
        # Add the recently and dinamically created task to a list
        list_of_creates.append(create_table)
    
    # Create an end dummy task
    end_operator = DummyOperator(task_id='Stop_Subdag_execution',
                                 dag=dag)
    
    # Schedule tasks of the subdags
    start_operator >> list_of_creates
    list_of_creates >> end_operator
    
    return dag