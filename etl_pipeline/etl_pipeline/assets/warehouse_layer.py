from dagster import asset, AssetIn, Output
import pandas as pd

COMPUTE_KIND = "Postgres"
LAYER = "warehouse"

@asset(
    io_manager_key="psql_io_manager",
    ins={
        "gold_order_summary": AssetIn(key_prefix=["gold", "ecommerce"]),
    },
    key_prefix=["public"],
    group_name=LAYER,
    compute_kind=COMPUTE_KIND
)
def brazillian_ecom_summary(context, gold_order_summary) -> Output[pd.DataFrame]:

    return Output(
        gold_order_summary,
        metadata={
            "table": "brazillian_ecom_summary",
            "records count": len(gold_order_summary)
        }
    )
