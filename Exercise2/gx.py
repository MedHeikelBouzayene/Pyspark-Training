import great_expectations as gx
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from pyspark.sql import SparkSession



context = gx.get_context()
spark = SparkSession.builder.appName("great-expectation").getOrCreate()

df = spark.read.options(header = 'True', inferSchema = 'True').csv('C:/Users/mhbouzayenne/Desktop/UDEMY COURSE/Pyspark-Training/Exercice2/df1.csv')

datasource_config = {
    "name": "version-0.15.50 my_spark_dataframe",
    "class_name": "Datasource",
    "execution_engine": {"class_name": "SparkDFExecutionEngine"},
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"],
            
        }
    },
}
context.add_datasource(**datasource_config)

expectation_suite = context.add_or_update_expectation_suite(
    expectation_suite_name="version-0.15.50 test_suite"
)

expectation_configuration = ExpectationConfiguration(
    expectation_type = "expect_column_values_to_be_between",
    kwargs = {
        "column": "date",
        "mostly": 1,
        "min_value": "2021-01-01",
        "max_value": "2021-03-09",
    },
    meta = {
        "notes": {
            "format": "markdown",
            "content": "Some clever comment about this expectation. **Markdown** `Supported`",
        }
    },
)
expectation_suite.add_expectation(expectation_configuration=expectation_configuration)
context.save_expectation_suite(expectation_suite=expectation_suite)

checkpoint_configuration = {
    "name":'mycheckpoint',
    "config_version":1,
    "class_name":'SimpleCheckpoint',
    "validations":[
        {
            "batch_request": {
                "datasource_name":"version-0.15.50 my_spark_dataframe",
                "data_connector_name":"default_runtime_data_connector_name",
                "data_asset_name":"version-0.15.50 <your_meangingful_name>",  # This can be anything that identifies this data_asset for you
            }, 
            'expectation_suite_name': "version-0.15.50 test_suite"
        }
    ]
}
checkpoint = context.add_or_update_checkpoint(**checkpoint_configuration)

checkpoint_result = context.run_checkpoint(
    checkpoint_name= 'mycheckpoint',
    batch_request={
        "runtime_parameters":{"batch_data": df},
        "batch_identifiers": {
            "default_identifier_name": "<your>"
        },
    },
)
print(checkpoint_result)
spark.stop()