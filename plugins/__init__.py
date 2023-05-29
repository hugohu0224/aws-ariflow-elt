from __future__ import division, absolute_import, print_function
from airflow.plugins_manager import AirflowPlugin

from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator



# Defining the plugin class
class MyPlugin(AirflowPlugin):
    name = "my_plugin"
    operators = [
        StageToRedshiftOperator,
        DataQualityOperator,
        LoadFactOperator,
        LoadDimensionOperator,
    ]
