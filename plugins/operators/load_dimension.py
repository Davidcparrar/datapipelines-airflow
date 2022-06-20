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

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.append:
            self.log.info("Deleting existing records")
            redshift.run("TRUNCATE public.{}".format(self.table))

        self.log.info("Update Data")
        sql_query = "INSERT INTO public.{}".format(self.table) + self.select

        redshift.run(sql_query)
