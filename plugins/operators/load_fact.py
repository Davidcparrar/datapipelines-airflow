from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = "#F98866"
    insert_sql = """
    INSERT INTO {table} (
        playid,
        start_time, 
        userid,
        level,
        songid,
        artistid,
        sessionid,
        location,
        user_agent
    )"""

    @apply_defaults
    def __init__(
        self,
        table="",
        staging_songs="",
        staging_events="",
        select="",
        redshift_conn_id="",
        *args,
        **kwargs
    ):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.staging_songs = staging_songs
        self.staging_events = staging_events
        self.select = select
        self.redshift_conn_id = redshift_conn_id

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

        self.log.info("Constructing query")
        sql_query = (
            LoadFactOperator.insert_sql.format(table=self.table)
            + "\n"
            + self.select.format(
                staging_songs=self.staging_songs,
                staging_events=self.staging_events,
            )
        )
        self.log.info("Inserting records into {}".format(self.table))
        redshift.run(sql_query)
