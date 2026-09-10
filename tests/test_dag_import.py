import os

from airflow.models import DagBag

DAGS_FOLDER = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dags"
)


def test_dag_bag_has_no_import_errors():
    dag_bag = DagBag(dag_folder=DAGS_FOLDER, include_examples=False)
    assert not dag_bag.import_errors, (
        f"DAG import errors found: {dag_bag.import_errors}"
    )


def test_retail_pipeline_dag_loads_with_expected_tasks():
    dag_bag = DagBag(dag_folder=DAGS_FOLDER, include_examples=False)
    dag = dag_bag.dags["retail_analytics_pipeline"]

    task_ids = set(dag.task_ids)
    assert task_ids == {
        "load_csv_raw", "api_ingest", "cast_api_raw", "load_postgres",
        "dbt_run", "dbt_test",
    }


def test_load_postgres_depends_on_load_csv_raw_and_cast_api_raw():
    dag_bag = DagBag(dag_folder=DAGS_FOLDER, include_examples=False)
    dag = dag_bag.dags["retail_analytics_pipeline"]

    load_task = dag.get_task("load_postgres")
    upstream_ids = {t.task_id for t in load_task.upstream_list}
    assert upstream_ids == {"load_csv_raw", "cast_api_raw"}


def test_dbt_test_runs_after_dbt_run_which_runs_after_load_postgres():
    dag_bag = DagBag(dag_folder=DAGS_FOLDER, include_examples=False)
    dag = dag_bag.dags["retail_analytics_pipeline"]

    dbt_run_task = dag.get_task("dbt_run")
    assert {t.task_id for t in dbt_run_task.upstream_list} == {"load_postgres"}

    dbt_test_task = dag.get_task("dbt_test")
    assert {t.task_id for t in dbt_test_task.upstream_list} == {"dbt_run"}
