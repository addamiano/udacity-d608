from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base import BaseHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        FORMAT AS JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="auto",
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 region="us-east-1",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.region = region

    def execute(self, context):
        # Get AWS credentials using BaseHook instead of AwsHook
        aws_conn = BaseHook.get_connection(self.aws_credentials_id)
        access_key = aws_conn.login
        secret_key = aws_conn.password
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Clearing data from destination Redshift table: {self.table}")
        redshift.run(f"DELETE FROM {self.table}")
        
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            access_key,
            secret_key,
            self.region,
            self.json_path
        )
        
        self.log.info(f"Executing COPY command for {self.table}")
        redshift.run(formatted_sql)
        
        self.log.info(f"StageToRedshiftOperator complete for table {self.table}")
