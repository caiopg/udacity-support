from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_path",)
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        JSON '{}'      
        """
    
    @apply_defaults
    def __init__(self,
                # aws_access_key_id=None,
                # aws_secret_access_key=None,
                # postgres_conn_id = None,
                table="",
                # s3_path = None,
                # region = None,
                # json_format = None,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # self.aws_access_key_id = aws_access_key_id
        # self.aws_secret_access_key = aws_secret_access_key
        # self.postgres_conn_id = postgres_conn_id
        self.aws_access_key_id = None
        self.aws_secret_access_key = None
        self.postgres_conn_id = None
        self.table = None
        self.s3_path = None
        self.region = None
        self.json_format = None
        # self.table = table
        # self.s3_path = s3_path
        # self.region = region
        # self.json_format = json_format

    def execute(self, context):
        self.log.info(f"Started StageToRedshiftOperator")

        redshift_hook = PostgresHook(self.postgres_conn_id)

        self.log.info(f"Copying data from '{self.s3_path}' to '{self.table}'")
        redshift_hook.run(StageToRedshiftOperator.copy_sql.format(
            self.table,
            self.s3_path,
            self.aws_access_key_id,
            self.aws_secret_access_key,
            self.region,
            self.json_format
        ))
        self.log.info(f"Finished copying data from '{self.s3_path}' to '{self.table}'")
