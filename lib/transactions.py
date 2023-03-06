import pyspark.sql.functions as sqlf


def group_transactions(columns=["mbr_id"]):
    def apply(tx):
        return tx.groupBy(*columns).agg(sqlf.sum("amount").alias("amount"))

    return apply
