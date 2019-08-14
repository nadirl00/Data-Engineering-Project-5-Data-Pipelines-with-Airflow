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
        """
        The data quality operator is used to run checks on the data itself. 
        The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. 
        For each the test, the test result and expected result needs to be checked and if there is no match, the operator should raise an exception and the task should retry and fail eventually.
        It uses the following parameters:
        - redshift_conn_id : contains the connection details to the data warehouse in Amazon Redshift (from in Airflow)
        - retries : number of retries before raising an exception
        - tables : a dictionary containg table names as keys and a list of tests to run on these tables as values associated to the keys.
        """     
        self.log.info('Testing data quality')
        # coneecting to the redshift
        redshift_hook = PostgresHook(self.redshift_conn_id)

        # for each table in the dictionnary
        for table in self.tables:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            # check that there are records in the table
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            # check that there are rows in the table
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            # for each column with the NOT NULL constraint
            columnList =  self.tables[table][0]
            for column in columnList:
                nullCount = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL")
                # check the NOT NULL constraint 
                if len(nullCount) < 1 or nullCount[0][0] > 0:
                    raise ValueError(f"Data quality check failed. {column} column contains {nullCount[0][0]} NULL values")
            # for each query in the query/exepected result dictionary
            queryResultDictionary = self.tables[table][1]
            for query in queryResultDictionary:
                # run the query and get the number of rows for the result
                count = redshift_hook.get_records(query)[0][0]
                # verify if the number of rows matches the expected number of rows.
                if  count != queryResultDictionary[query]:
                    raise ValueError(f"Data quality check failed. Query {query} contained {count} rows. {queryResultDictionary[query]} was expected")
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
        