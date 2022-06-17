from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = "#358140"
    template_fields = ("data_path",)
    copy_sql = copy_sql = """
        COPY {table} FROM {s3_path}
        ACCESS_KEY_ID '{access_key}'
        SECRET_ACCESS_KEY '{seecret_key}'
        REGION '{region}'
    """

    @apply_defaults
    def __init__(
        self,
        table="",
        s3_key="",
        json_path=None,
        region="",
        s3_bucket="",
        aws_credentials_id="",
        redshift_conn_id="",
        *args,
        **kwargs
    ):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.region = region
        self.s3_key = s3_key
        self.s3_bucket = s3_bucket
        self.json_path = json_path
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            table=self.table,
            s3_path=s3_path,
            access_key=credentials.access_key,
            secret_key=credentials.secret_key,
            region=self.region,
        )

        if self.json_path is not None:
            formatted_sql = formatted_sql + " JSON s3://{}/{}".format(
                self.s3_bucket, self.json_path
            )
        print(formatted_sql)
        # redshift.run(formatted_sql)
