from airflow.models.baseoperator import BaseOperator


class CustomDagRunOperator(BaseOperator):
    def __init__(self, trigger_dag_id,
                 wait_for_completion=True,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.trigger_dag_id = trigger_dag_id
        self.wait_for_completion = wait_for_completion

    def execute(self, context):
        raise NotImplementedError()

# tasks.append(TriggerDagRunOperator(
#                 task_id=f"task_run_{dag.dag_id}",
#                 trigger_dag_id=dag.dag_id, wait_for_completion=True,
#                 poke_interval=10,
#                 trigger_rule='none_failed',
#             ))
