def DBT_0():
    from datetime import timedelta
    from airflow.operators.bash import BashOperator

    return BashOperator(
        task_id = "DBT_0",
        bash_command = "set -euxo pipefail; tmpDir=`mktemp -d`; git clone https://github.com/abhisheks-prophecy/test_repo_sql --branch main --single-branch $tmpDir; cd $tmpDir/perftest/snowfalke/randome2etests; dbt deps --profile run_profile_snowflake; dbt seed --profile run_profile_snowflake; dbt run --profile run_profile_snowflake; ",
        env = {"DBT_PROFILES_DIR" : "/home/airflow/gcs/data", "DBT_FULL_REFRESH" : "true"},
        append_env = True,
    )
