from contextlib import closing
from typing import Callable, Iterable, Optional, Union

from airflow.providers.postgres.hooks.postgres import PostgresHook


class PostgresMonitorHook(PostgresHook):
    """The TalosOperator adds some observability capabilities to the PostgresHook.

    Specifically it returns the number of rows impacted by the query and logs query execution information.
    """

    def run(
        self,
        sql: Union[str, list],
        autocommit: bool = False,
        parameters: Optional[Union[dict, Iterable]] = None,
        handler: Optional[Callable] = None,
    ) -> Optional[int]:
        """Runs a command or a list of commands.

        The user can also pass a list of sql statements to the sql parameter to get them to execute sequentially.

        Args:
            sql:        The sql statement or a list of sql statements to execute.
            autocommit: Whether  the autocommit is activated or not. Defaults to False.
            parameters: The parameters to render the SQL query with.
            handler:    The result handler which is called with the result of each statement.

        Returns:
            The number of rows affected by the query.
        """
        if isinstance(sql, str):
            sql = [sql]
        affected = 0
        with closing(self.get_conn()) as conn:
            if self.supports_autocommit:
                self.set_autocommit(conn, autocommit)

            with closing(conn.cursor()) as cur:
                for sql_statement in sql:
                    self.log.info(
                        f"Running statement: {sql_statement}, parameters: {parameters}",
                    )

                    if handler is not None:
                        handler(cur)

                    if parameters:
                        cur.execute(sql_statement, parameters)
                    else:
                        cur.execute(sql_statement)
                    if hasattr(cur, "rowcount"):
                        self.log.info(f"Rows affected: {cur.rowcount}")
                        self.log.info(f"status message: {cur.statusmessage}")
                        affected += cur.rowcount

            # If autocommit was set to False for db that supports autocommit, or if db does not supports autocommit,
            # we do a manual commit.
            if not self.get_autocommit(conn):
                conn.commit()
            return affected
