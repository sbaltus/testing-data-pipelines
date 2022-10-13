from pipelines.dags.etl_dept_and_population import dag


def test_valid_dag():
    assert dag
    assert dag.tasks
    assert len(dag.tasks) == 5


# test request call at dag instanciation
