"""DAG integrity tests. Skipped automatically where Airflow is not installed."""
import importlib
import os

import pytest

pytest.importorskip('airflow')

from airflow.models import DagBag  # noqa: E402

DAG_FOLDER = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'dags')


@pytest.fixture(scope='module')
def dagbag():
    return DagBag(dag_folder=DAG_FOLDER, include_examples=False)


def test_dag_imports_without_errors(dagbag):
    assert dagbag.import_errors == {}


def test_dag_structure_keeps_original_ids(dagbag):
    dag = dagbag.dags.get('etl_reddit_pipeline')
    assert dag is not None and dag.catchup is False and dag.max_active_runs == 1
    ids = set(dag.task_ids)
    assert {'run_reddit_pipeline', 's3_upload', 'postgres_load', 'cleanup_local_files'} <= ids
    assert dag.get_task('run_reddit_pipeline').downstream_task_ids == {'s3_upload', 'postgres_load'}
    assert dag.get_task('cleanup_local_files').upstream_task_ids == {'s3_upload', 'postgres_load'}
    assert dag.default_args['retries'] >= 1 and dag.default_args['on_failure_callback']


def test_file_name_is_templated_not_frozen_at_parse_time(dagbag):
    """Regression: the original used datetime.now() at parse time."""
    task = dagbag.dags.get('etl_reddit_pipeline').get_task('run_reddit_pipeline')
    assert '{{ ds_nodash }}' in task.op_kwargs['file_name'] and 'op_kwargs' in task.template_fields


def test_s3_task_points_at_the_real_extract_task_id(dagbag):
    """Regression: the original pulled XCom from 'reddit_extraction', which never existed."""
    dag = dagbag.dags.get('etl_reddit_pipeline')
    extract_id = dag.get_task('s3_upload').op_kwargs['extract_task_id']
    assert extract_id in dag.task_ids


def test_multiple_subreddits_create_parallel_branches(monkeypatch):
    monkeypatch.setenv('ETL_SUBREDDITS', 'dataengineering, python')
    import utils.constants as constants
    importlib.reload(constants)
    try:
        bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        assert bag.import_errors == {}
        ids = set(bag.dags['etl_reddit_pipeline'].task_ids)
        assert {'run_reddit_pipeline__dataengineering', 'run_reddit_pipeline__python',
                's3_upload__python', 'postgres_load__python', 'cleanup_local_files'} <= ids
    finally:
        monkeypatch.delenv('ETL_SUBREDDITS')
        importlib.reload(constants)
