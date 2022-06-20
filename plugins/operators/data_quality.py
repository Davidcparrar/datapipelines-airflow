from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        tables=None,
        table_nnull_columns=None,
        *args,
        **kwargs,
    ):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.table_nnull_columns = table_nnull_columns

    def check_rows(self, redshift):
        """Check for empty tables

        Args:
            redshift (PostgresHook): Redshift connection

        Raises:
            ValueError: When no records are found
        """
        for table in self.tables:
            records = redshift.get_records(
                f"SELECT COUNT(*) FROM public.{table}"
            )

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(
                    f"Data quality check failed. {table} returned no results"
                )

    def check_null(self, redshift):
        """Check for nulls in columns

        Args:
            redshift (PostgresHook): Redshift connection

        Raises:
            ValueError: When nulls are found
        """
        for table, column in self.table_nnull_columns:
            records = redshift.get_records(
                f"SELECT COUNT(*) - COUNT({column}) FROM public.{table}"
            )

            if len(records) < 1 or not records[0][0] == 0:
                raise ValueError(
                    f"Data quality check failed. {column} from {table} may contain null values"
                )

    def execute(self, context):
        self.log.info("Running quality checks")

        keepalive_kwargs = {
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 5,
            "keepalives_count": 5,
        }
        redshift = PostgresHook(
            postgres_conn_id=self.redshift_conn_id, **keepalive_kwargs
        )

        self.check_rows(redshift)
        self.check_null(redshift)
