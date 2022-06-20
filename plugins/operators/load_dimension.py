from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = "#80BD9E"

    @apply_defaults
    def __init__(
        self,
        table="",
        select="",
        redshift_conn_id="",
        append=False,
        *args,
        **kwargs
    ):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.select = select
        self.redshift_conn_id = redshift_conn_id
        self.append = append

    def execute(self, context):
        self.log.info("Creating connection")

        keepalive_kwargs = {
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 5,
            "keepalives_count": 5,
        }
        redshift = PostgresHook(
            postgres_conn_id=self.redshift_conn_id, **keepalive_kwargs
        )

        if not self.append:
            self.log.info("Deleting existing records")
            redshift.run("TRUNCATE public.{}".format(self.table))

        self.log.info("Update Data")
        sql_query = "INSERT INTO public.{}".format(self.table) + self.select

        # self.log.info(sql_query)
        redshift.run(sql_query)
