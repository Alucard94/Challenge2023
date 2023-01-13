import os
from airflow import DAG
from datetime import datetime
from airflow.contrib.operators.ecs_operator import ECSOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import urllib.parse

default_args = {
    "owner": "Augusto",
    "depends_on_past": False,
    "start_date": datetime(2022, 2, 12),
    "retries": 0,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    "daily_inc_run",
    default_args=default_args,
    description="Daily ingest db tables",
    schedule_interval="30 2 * * *",
    concurrency=5,
    max_active_runs=1,
    is_paused_upon_creation=True,
    catchup=False
)

commands = { 
"INCREMENTAL": " --loadMethod INCREMENTAL"
}

dop = DummyOperator(task_id='dop', dag=dag)

for tab, cmd_str in commands.items():
    overwrite_command=cmd_str.split()
    overwrite_command=[urllib.parse.unquote(x) for x in overwrite_command]
    
    ox_batch = ECSOperator(
        task_id="inc_run_"+tab,
        dag=dag,
        aws_conn_id="aws_default",
        cluster="loka-cluster",
        task_definition="loka-test:1",
        launch_type="EC2",
        overrides={
            "containerOverrides": [
                {
                    "name": "loka-test-container",
                    "command": overwrite_command
                }
            ]     
        },
        awslogs_group="/ecs/loka-test",
        awslogs_stream_prefix="ecs/loka-test-container",
    )

    dop >> ox_batch
