from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower
from collections import defaultdict
import logging
from pipeline.utilities import normalize_key, try_mapping

log = logging.getLogger("boxplot_pipeline")

def build_final_dataframe(
    df_alert_metadata: DataFrame,
    df_threshold_metadata: DataFrame,
    df_carveout_metadata: DataFrame,
    df_var_mapping: DataFrame,
    df_carveout_data: DataFrame,
    alert_type_id: int
):
    log.info(f"Processing metadata for ALERT_TYPE_ID : {alert_type_id}")

    alert_metadata_row = (
        df_alert_metadata
        .filter(col("alert_type_id") == alert_type_id)
        .selectExpr(
            "min(alert_type) as alert_type",
            "min(alert_type_value) as alert_type_value",
            "min(impala_table_name) as impala_table_name",
            "min(clib_file_path) as clib_file_path"
        )
        .collect()[0]
    )
    alert_metadata = alert_metadata_row.asDict()

    threshold_metadata = (
        df_threshold_metadata
        .filter((col("alert_type_id") == alert_type_id) &
                (col("category").isin("TH", "TH.CALC", "FILTER")) &
                col("th_name").isNotNull() &
                col("th_logic").isNotNull())
        .collect()
    )

    carveout_metadata = (
        df_carveout_metadata
        .filter((col("alert_type_id") == alert_type_id) & (col("category") == "CARVEOUT"))
        .select("th_name", "clib_phy_column_name")
        .distinct()
        .collect()
    )

    var_mapping_rows = (
        df_var_mapping
        .filter((col("alert_type_id") == alert_type_id) &
                (col("display_flag") == 'Y') &
                (col("type") == 'TH'))
        .collect()
    )

    carveout_data_rows = (
        df_carveout_data
        .filter((col("alert_type_id") == alert_type_id) & (col("type") == 'TH'))
        .collect()
    )

    log.info("Metadata queries completed. Starting transformation.")

    thresholds = defaultdict(dict)
    for row in threshold_metadata:
        category = row["CATEGORY"]
        name = row["TH_NAME"]
        if category == "TH":
            thresholds[name]["clb_phy_column_name"] = row["CLIB_PHY_COLUMN_NAME"]
            thresholds[name]["th_logic"] = row["TH_LOGIC"]
        elif category == "TH.CALC":
            thresholds[name]["th_calc"] = row["CLIB_PHY_COLUMN_NAME"]
        elif category == "FILTER":
            thresholds[name]["th_condition"] = row["TH_CONDITION"]

    carveout_names = [r["TH_NAME"] for r in carveout_metadata]
    carveout_clb_names = [r["CLIB_PHY_COLUMN_NAME"] for r in carveout_metadata]
    carveout_column_map = {r["TH_NAME"]: r["CLIB_PHY_COLUMN_NAME"] for r in carveout_metadata}

    display_to_var = {
        normalize_key(row["DISPLAYNAME"]): normalize_key(row["FIELDNAME"])
        for row in var_mapping_rows
    }

    carveout_var_fields = {
        row["CLIB_PHY_COLUMN_NAME"]: try_mapping(display_to_var, row["TH_NAME"])
        for row in carveout_metadata
    }

    threshold_var_field = try_mapping(display_to_var, "Name")

    carveout_combinations_by_threshold = defaultdict(list)
    for row in carveout_data_rows:
        actual_keys = {normalize_key(k): k for k in row.asDict().keys()}
        threshold = row.asDict().get(actual_keys.get(threshold_var_field.lower()))
        if not threshold:
            continue

        carveout_values = {}
        for name in carveout_names:
            column = carveout_column_map[name]
            mapped_var = carveout_var_fields.get(column)
            key = actual_keys.get(mapped_var.lower()) if mapped_var else None
            if key:
                carveout_values[column] = row[key]
        carveout_combinations_by_threshold[threshold].append(carveout_values)

    rows = []
    for th_name in thresholds:
        th_info = thresholds[th_name]
        carveout_rows = carveout_combinations_by_threshold.get(th_name, [{}])
        for carveout_vals in carveout_rows:
            row = {
                "alert_type_id": alert_type_id,
                "alert_type": alert_metadata["alert_type"],
                "alert_type_value": alert_metadata["alert_type_value"],
                "impala_table_name": alert_metadata["impala_table_name"],
                "clib_file_path": alert_metadata["clib_file_path"],
                "th_name": th_name,
                "clb_phy_column_name": th_info.get("clb_phy_column_name"),
                "th_logic": th_info.get("th_logic"),
                "th_calc": th_info.get("th_calc"),
                "th_condition": th_info.get("th_condition")
            }
            row.update(carveout_vals)
            rows.append(row)

    final_df = spark.createDataFrame(rows)
    log.info(f"Metadata transformation complete. Final shape: {final_df.count()} rows")

    return (
        final_df,
        alert_metadata["clib_file_path"],
        alert_metadata["alert_type"],
        alert_metadata["alert_type_value"],
        carveout_clb_names
    )
