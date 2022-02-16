# -*- coding: utf-8 -*-
from airflow import DAG
import requests
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.executors.sequential_executor import SequentialExecutor
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

extract = []

temporary = []


def load_extract(parent_dag_name, child_dag_name, args):

    dag_subdag = DAG(
        dag_id='{0}.{1}'.format(parent_dag_name, child_dag_name),
        default_args=args,
        description='Subdag {0} da dag {1}'.format(child_dag_name, parent_dag_name),
        catchup=False

    )

    with dag_subdag:

        for i in ['categorias', 'vendas', 'vendedores']:
            
            command = """
            sudo python3 /home/ec2-user/projeto_bix/extract_*TABLE*.py
            """
            command = command.replace('*TABLE*', i)

            operador = SSHOperator(
                task_id='{0}'.format(i),
                ssh_conn_id='ssh_maquina_etl',
                command=command,
                dag=dag_subdag)

            extract.append(operador)

            

    return dag_subdag


def load_upload(parent_dag_name, child_dag_name, args):

    dag_subdag = DAG(
        dag_id='{0}.{1}'.format(parent_dag_name, child_dag_name),
        default_args=args,
        description='Subdag {0} da dag {1}'.format(child_dag_name, parent_dag_name),
        catchup=False

    )

    with dag_subdag:

        for i in ['category', 'employee', 'sales']:
            
            command = """
            sudo python3 /home/ec2-user/projeto_bix/upload_tmp_*TABLE*.py
            """
            command = command.replace('*TABLE*', i)

            operador = SSHOperator(
                task_id='{0}'.format(i),
                ssh_conn_id='ssh_maquina_etl',
                command=command,
                dag=dag_subdag)

            temporary.append(operador)

            

    return dag_subdag


default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(days=2),
    'email': ['lucas.c.franca@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=3)
}


with DAG('Cadeia_diaria_projeto_bix', schedule_interval='0 3 * * *', default_args=default_args, catchup=False,
         tags=['projeto_bix']) as dag:

    etapa_extract = SubDagOperator(
        task_id="Extract",
        subdag=load_extract(
            parent_dag_name="Cadeia_diaria_projeto_bix",
            child_dag_name="Extract",
            args=default_args
        ),
        default_args=default_args
    )

    etapa_tmp = SubDagOperator(
        task_id="Load_tmp",
        subdag=load_upload(
            parent_dag_name="Cadeia_diaria_projeto_bix",
            child_dag_name="Load_tmp",
            args=default_args
        ),
        default_args=default_args
    )
#-----------------------------------------------send_to_s3--------------------------------------------------------
    command = """
    sudo python3 /home/ec2-user/projeto_bix/send_to_s3.py
    """
    send_s3 = SSHOperator(
            task_id='Send_to_s3',
            ssh_conn_id='ssh_maquina_etl',
            command=command,
            dag=dag)
#---------------------------------------------insert_final--------------------------------------------------------------
    command = """  
    sudo python3 /home/ec2-user/projeto_bix/insert_visualization.py
    """
    load_visu = SSHOperator(
            task_id='Insert_in_visualization',
            ssh_conn_id='ssh_maquina_etl',
            command=command,
            dag=dag)
#----------------------------------------------END_Docapi---------------------------------------------------------

    etapa_extract >> send_s3 >> etapa_tmp >> load_visu

    extract[0] >> extract[1] >> extract[2] 
    
    temporary[0] >> temporary[1] >> temporary[2]


