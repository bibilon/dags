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


from collections.abc import Mapping
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any

from kubernetes.client import CoreV1Api, CustomObjectsApi, models as k8s
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes import pod_generator
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook, _load_body_to_dict
from airflow.providers.cncf.kubernetes.kubernetes_helper_functions import add_unique_suffix
from airflow.providers.cncf.kubernetes.operators.custom_object_launcher import CustomObjectLauncher
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.pod_generator import MAX_LABEL_LEN, PodGenerator
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodManager
from airflow.utils.helpers import prune_dict
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
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

class CustomSparkKubernetesOperator(SparkKubernetesOperator):
    def manage_template_specs(self):
        print("application_file: ", self.application_file)
        if self.application_file:
            try:
                filepath = Path(self.application_file.rstrip()).resolve(strict=True)
            except (FileNotFoundError, OSError, RuntimeError, ValueError):
                application_file_body = self.application_file
            else:
                application_file_body = filepath.read_text()
            template_body = _load_body_to_dict(application_file_body)
            if not isinstance(template_body, dict):
                msg = f"application_file body can't transformed into the dictionary:\n{application_file_body}"
                raise TypeError(msg)
        elif self.template_spec:
            template_body = self.template_spec
        else:
            raise AirflowException("either application_file or template_spec should be passed")
        if "spark" not in template_body:
            template_body = {"spark": template_body}
        return template_body

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
