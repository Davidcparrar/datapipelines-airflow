from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        template=None,
        *args,
        **kwargs,
    ):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.template = template

    def process_template(self, redshift):
        """Process template passed as argument for quality checks

        Args:
            redshift (PostgresHook): Connection to Redshift Database

        Raises:
            ValueError: When template has not been defined by the user
            ValueError: When the quality check fails
            ValueError: When non supported operator is passed on the template
        """
        if self.template is None:
            raise ValueError("No template was passed down")

        for quality_check in self.template:
            self.log.info(f"Processing {quality_check['name']} quality check")
            formats = quality_check.get("format")
            for format in formats:
                records = redshift.get_records(
                    quality_check.get("sql").format(**format)
                )

                if quality_check.get("operator") == "eq":
                    if not records or not records[0][0] == quality_check.get(
                        "result"
                    ):
                        raise ValueError(f"Data quality check failed")
                elif quality_check.get("operator") == "gt":
                    if not records or not records[0][0] > quality_check.get(
                        "result"
                    ):
                        raise ValueError(f"Data quality check failed")
                elif quality_check.get("operator") == "lt":
                    if not records or not records[0][0] < quality_check.get(
                        "result"
                    ):
                        raise ValueError(f"Data quality check failed")
                elif quality_check.get("operator") == "gte":
                    if not records or not records[0][0] >= quality_check.get(
                        "result"
                    ):
                        raise ValueError(f"Data quality check failed")
                elif quality_check.get("operator") == "lte":
                    if not records or not records[0][0] <= quality_check.get(
                        "result"
                    ):
                        raise ValueError(f"Data quality check failed")
                else:
                    raise ValueError(f"Empty or Non supported operator")

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

        self.process_template(redshift)
