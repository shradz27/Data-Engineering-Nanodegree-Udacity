from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_check_functions=[],
                 table=[],
                 expected_result=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_check_functions = dq_check_functions
        self.table=table
        self.expected_result=expected_result


    def execute(self, context):
        self.log.info('Running data quality checks')
        
        self.log.info('Fetching redshift hook')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        checks = zip(self.data_check_query, self.table, self.expected_result)
        for check in checks:
            try:
                redshift.run(check[0].format(check[1])) == check[2]
                self.log.info('Data quality check passed.')
            except:
                self.log.info('Data quality check failed.')
                raise AssertionError('Data quality check failed.')        
 