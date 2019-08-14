from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables={},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for table in self.tables:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            columnList =  self.tables[table][0]
            for column in columnList:
                nullCount = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL")
                if len(nullCount) < 1 or nullCount[0][0] > 0:
                    raise ValueError(f"Data quality check failed. {column} column contains {nullCount[0][0]} NULL values")
            
            queryResultDictionary = self.tables[table][1]
            for query in queryResultDictionary:
                count = redshift_hook.get_records(query)[0][0]
                if  count != queryResultDictionary[query]:
                    raise ValueError(f"Data quality check failed. Query {query} contained {count} rows. {queryResultDictionary[query]} was expected")
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
        
