import datetime as dt

import pyspark.sql.functions as sqlf

import spex.lib.base as base
import spex.lib.transactions as tx


def create_fancy_pipeline(member_data):
    return base.pipe(
        # base.log("Input"),
        base.select("mbr_id", "amount", "date"),
        base.filter(sqlf.col("date") >= sqlf.lit(dt.date(2020, 1, 1))),
        base.join(member_data, "mbr_id"),
        # base.log("After select/filter"),
        tx.group_transactions(columns=["hh_id"]),
        # base.log("Result"),
    )
