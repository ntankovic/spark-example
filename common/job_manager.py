import yaml
from pyspark import SparkContext
from pyspark.sql import SparkSession


class JobManager:
    def __init__(self, config_path=None, env="dev"):
        with open(config_path) as file:
            self.config = yaml.load(file, Loader=yaml.FullLoader)

        self.env = env
        self.sc = SparkContext.getOrCreate()
        self.spark = SparkSession.builder.appName("spex-app").getOrCreate()

    def read(self, table_name):
        table_info = self.config["paths"][table_name]
        table_type = table_info["format"]

        if table_type == "csv":
            df = self.spark.read.csv(
                table_info["path"],
                sep=table_info.get("sep", ","),
                header=table_info.get("header", True),
                escape=table_info.get("escape", "\\"),
                # schema=SCHEMAS.get(table_name),
                mode="FAILFAST",
                ignoreLeadingWhiteSpace=True,
                ignoreTrailingWhiteSpace=True,
            )
        elif table_type == "parquet":
            df = self.spark.read.parquet(table_info["path"])

        return df
