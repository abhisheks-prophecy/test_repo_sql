def DBT_0():
    from datetime import timedelta
    from airflow.operators.bash import BashOperator

    return BashOperator(
        task_id = "DBT_0",
        bash_command = "set -euxo pipefail; tmpDir=`mktemp -d`; git clone https://github.com/abhisheks-prophecy/test_repo_sql --branch dev_databricks_perftest --single-branch $tmpDir; cd $tmpDir/perftest/databricks/e2etests1; dbt deps --profile run_profile; dbt seed --profile run_profile; dbt run --profile run_profile; dbt test --profile run_profile; ",
        env = {"DBT_PROFILES_DIR" : "/home/airflow/gcs/data", "DBT_FULL_REFRESH" : "true"},
        append_env = True,
    )
