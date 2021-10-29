import datetime

from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.contrib.sensors import gcs_sensor
from airflow.utils import trigger_rule


default_dag_args = {
    'depends_on_past': False,
    'start_date': datetime.datetime.combine(datetime.datetime.today(), datetime.datetime.min.time()),  # today
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': models.Variable.get('gcp_project')
}

dataset_path_in_bucket = "{}/{}".format(models.Variable.get('dataset_name'), '{{ execution_date.strftime("%Y/%m/%d/%H") }}')


with models.DAG(
        'composer_lab',
        schedule_interval="@hourly",
        default_args=default_dag_args,
        catchup=False
) as dag:

    check_dataset_update = gcs_sensor.GoogleCloudStorageObjectSensor(
        task_id='success_sensor_dataproc_hadoop',
        bucket=models.Variable.get('input_bucket_name'),
        object="{}/{}".format(dataset_path_in_bucket, '_SUCCESS'),
        poke_interval=60,  # sec
        timeout=60 * 60 * 2,  # sec, i.e 2 hours
    )

    create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        cluster_name='composer-hadoop-tutorial-cluster-{{ ds_nodash }}',
        num_workers=2,
        region=models.Variable.get('gce_region'),
        zone=models.Variable.get('gce_zone'),
        master_machine_type='n1-standard-2',
        worker_machine_type='n1-standard-2'
    )

    run_dataproc_hadoop = dataproc_operator.DataProcPySparkOperator(
        task_id='run_dataproc_hadoop',
        main="gs://{}".format(models.Variable.get('script_path')),
        arguments=[
            '--input_path', 'gs://{}/{}'.format(models.Variable.get('input_bucket_name'), dataset_path_in_bucket),
            '--out_path', 'gs://{}/{}'.format(models.Variable.get('out_bucket_name'), '{{ execution_date.strftime("%Y/%m/%d/%H") }}')
        ],
        cluster_name='composer-hadoop-tutorial-cluster-{{ ds_nodash }}',
        region=models.Variable.get('gce_region'),
        zone=models.Variable.get('gce_zone'),
    )

    delete_dataproc_cluster = dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        cluster_name='composer-hadoop-tutorial-cluster-{{ ds_nodash }}',
        region=models.Variable.get('gce_region'),
        zone=models.Variable.get('gce_zone'),
        # Setting trigger_rule to ALL_DONE causes the cluster to be deleted
        # even if the Dataproc job fails.
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE
    )

    # Define DAG dependencies.
    check_dataset_update >> create_dataproc_cluster >> run_dataproc_hadoop >> delete_dataproc_cluster
