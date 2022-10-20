from typing import List, Optional, Union

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.context import Context

from pipelines.helpers.time_utils import timer
from pipelines.hooks.postgres import PostgresMonitorHook
from pipelines.operators.exceptions import (
    PostgresMonitorOperatorCommandException,
)


class PostgresMonitorOperator(PostgresOperator):
    """The TalosOperator enriches the PostgresOperator by adding query observability feature."""

    ui_color = "#fde1fb"

    def __init__(
        self,
        *,
        fail_if_no_result: bool = False,
        work_mem: Optional[str] = None,
        **kwargs,
    ) -> None:
        """Allowing one more option to the PostgresOperator.

        Args:
            fail_if_no_result: if true, it will raise an error if no row has been affected.
            work_mem: the working memory to allocate during your query.
        """
        super().__init__(**kwargs)

        self.fail_if_no_result = fail_if_no_result
        self.work_mem = work_mem

        self._talos_hook = None

    @staticmethod
    def _get_sql(arg: Union[List[str], str]):
        if isinstance(arg, List):
            return arg
        if isinstance(arg, str) and not arg.startswith("["):
            return arg
        raise NotImplementedError(
            f"cannot convert this value {arg} to a valid SQL command."
        )

    @property
    def talos_hook(self):
        """Instantiate the TalosHook only once."""
        if not self._talos_hook:
            self._talos_hook = PostgresMonitorHook(
                postgres_conn_id=self.postgres_conn_id, schema=self.database
            )

        return self._talos_hook

    @timer
    def _execute_main_query(self) -> Optional[int]:
        """Execute the main PG query via the TalosHook."""
        self.log.info(f"Executing queries: {self.sql}")

        return self.talos_hook.run(
            self.sql, self.autocommit, parameters=self.parameters
        )

    def _push_metadata_into_xcoms(
        self, affected_rows: int, duration_ms: float, context: Context
    ) -> None:
        """Record execution duration and affected rows to the XComs."""
        self.xcom_push(
            context,
            key="execution_duration",
            value=round(duration_ms, 2),
        )
        self.xcom_push(context, key="affected_rows", value=affected_rows)

    def execute(self, context: Context) -> None:
        """Executes the given SQL query.

        Args:
            context: the dag execution context.
        """
        try:
            if self.work_mem:
                self.log.info(f"Setting working memory to {self.work_mem}")
                self.talos_hook.run(f"SET work_mem='{self.work_mem}';", self.autocommit)

            # We double-render the query field, so that we can have nested jinja templates within our query/sql field.
            self.sql = self.render_template(self._get_sql(self.sql), context)

            affected, duration_ms = self._execute_main_query()
        except Exception as ex:
            raise PostgresMonitorOperatorCommandException(ex)

        if self.fail_if_no_result and affected == 0:
            raise PostgresMonitorOperatorCommandException(
                "0 row affected during your query!"
            )

        self._push_metadata_into_xcoms(affected, duration_ms, context)
        self.log.info(
            f"{self.__class__.__name__} ran successfully in: {duration_ms / 1000:.2f}s"
        )
