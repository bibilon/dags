from __future__ import annotations
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from kubernetes.client import models as k8s
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.empty import EmptyOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

spark_template_spec = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
        "name": "spark-test",
        "namespace": "spark-jobs",
    },
    "spec": {
        "type": "Scala",
        "mode": "cluster",
        "image": "hongtt11/spark-aws-delta:3.3.202",
        "imagePullPolicy": "Always",
        "mainClass": "com.lifesup.test",
        "mainApplicationFile": "local:///opt/spark/jars/test123.jar",
        "sparkVersion": "3.3.3",
        "restartPolicy": {"type": "Never"},
        "driver": {
            "cores": 1,
            "coreLimit": "1000m",
            "memory": "4096m",
            "labels": {"version": "3.3.3"},
            "serviceAccount": "spark",
            "envVars": {
                "oraclePassword": "12345",
            },
        },
        "executor": {
            "instances": 2,
            "coreRequest": "1000m",
            "coreLimit": "1000m",
            "memory": "4096m",
            "labels": {"version": "3.3.3"},
        },
    },
}

default_params = {"start_date": "2022-01-01", "end_date": "2022-12-01"}
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


def push_sensor_status(**kwargs):
    ti = kwargs['ti']
    ti.xcom_push(key='return_value', value='success')
        
with DAG(
   'test_airflow_spark',
   default_args=default_args,
   description='simple dag',
   schedule_interval='45 3 * * *',
   start_date=datetime(2024, 5, 20),
   catchup=False,
   tags=['example13'],
   template_searchpath='/opt/airflow/dags/repo/'
) as dag:
   start = DummyOperator(task_id="start")
   end = DummyOperator(task_id="end")

   t1 = SparkKubernetesOperator(
       task_id='load_RP_SUB_PRE',
       retries=0,
       namespace='spark-jobs',
       application_file="test-spark.yaml",
       kubernetes_conn_id="myk8s",
       do_xcom_push=True,
       dag=dag
   )
   start >> t1 >> end
