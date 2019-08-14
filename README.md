# Data-Engineering-Project-5-Data-Pipelines-with-Airflow
Data Pipelines with Apache Airflow

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is **Apache Airflow**.

The aim of this project is to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. Since data quality plays a big part when analyses are executed on top the data warehouse, we incorporate tests against the  datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Implemented DAG:

The ETL pipeline has the following structure:
![](Images/example-dag.png)

## Operator details :

### Stage Operator
The stage operator loads any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided.
The parameters are:
- `redshift_conn_id`: contains the connection details to the data warehouse in Amazon Redshift (from in Airflow)
- `aws_credentials_id`: contains the credentials to connect to the S3 bucket (from in Airflow)
- `table`: contains the name of the table where the data from S3 is to be copied.
- `s3_bucket`: contains info on the S3 bucket where the information is located
- `s3_key`: contains info on the S3 bucket where the information is located
- `region`: contains the region where the S3 bucket is located
- `json`: JSON formatting parameter

The operator is in the file **plugins/operators/stage_redshift.py**

### Fact Operator:
With dimension and fact operators, you can utilize the provided SQL helper class to run data transformations. Most of the logic is within the SQL transformations and the operator is expected to take as input a SQL statement and target database on which to run the query against. You can also define a target table that will contain the results of the transformation.

- `redshift_conn_id` : contains the connection details to the data warehouse in Amazon Redshift (from in Airflow)
- `sql_statement` : contains the SQL statement to insert into the fact table.

The operator is in the file **plugins/operators/load_fact.py**

### Dimension Operators

Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the load. Thus, you could also have a parameter that allows switching between insert modes when loading dimensions. Fact tables are usually so massive that they should only allow append type functionality.

- `redshift_conn_id` : contains the connection details to the data warehouse in Amazon Redshift (from in Airflow)
- `table` : the name of the destination dimension table
- `sql_statement` : the SQL query to get the data to be inserted in the destination table
- `truncate-insert` : boolean indicating whether the dimension table is to be emptied before inserting the data 

The operator is in the file **plugins/operators/load_dimension.py**

### Data Quality Operator
The final operator is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result needs to be checked and if there is no match, the operator should raise an exception and the task should retry and fail eventually.

- `redshift_conn_id` : contains the connection details to the data warehouse in Amazon Redshift (from in Airflow)
- `retries` : number of retries before raising an exception
- `tables` : a dictionary containg table names as keys and a list of tests to run on these tables as values associated to the keys.
             The dictionary is structured is as follows:
             **Keys**: table names
             **Values**: List containing one list and one dictionary:
                        - The list contains the column names that are not null
                        - A dictionary containing key value pairs of queries and expected return values.

The operator is in the file **plugins/operators/data_quality.py**

## Requirements:

## Airflow configuration

Here, we'll use Airflow's UI to configure your AWS credentials and connection to Redshift.


Click on the **Admin** tab and select **Connections**.

![](Images/admin-connections.png)

Under **Connections**, select **Create**.

![](Images/create-connection.png)

On the create connection page, enter the following values:

**Conn Id:** Enter `aws_credentials`.

**Conn Type:** Enter `Amazon Web Services`.

**Login:** Enter your **Access key ID** from the IAM User credentials you downloaded earlier.

**Password:** Enter your **Secret access key** from the IAM User credentials you downloaded earlier.

Once you've entered these values, select **Save and Add Another**.

![](Images/connection-aws-credentials.png)

On the next create connection page, enter the following values:

**Conn Id**: Enter `redshift`.

**Conn Type**: Enter `Postgres`.

**Host**: Enter the endpoint of your Redshift cluster, excluding the port at the end. You can find this by selecting your cluster in the Clusters page of the Amazon Redshift console. See where this is located in the screenshot below. IMPORTANT: Make sure to **NOT** include the port at the end of the Redshift endpoint string.

**Schema**: Enter `dev`. This is the Redshift database you want to connect to.

**Login**: Enter `awsuser`.

**Password**: Enter the password you created when launching your Redshift cluster.

**Port**: Enter `5439`.

Once you've entered these values, select **Save**.

![](Images/cluster-details.png)

![](Images/connection-redshift.png)





