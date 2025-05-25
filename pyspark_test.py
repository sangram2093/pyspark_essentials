from collections import defaultdict
from pyspark.sql import Row
from pyspark.sql.functions import col
import logging

from pipeline.utilities import normalize_key, try_mapping

log = logging.getLogger("boxplot_pipeline")

def build_final_dataframe_spark(
    alert_type_id,
    df_alert_metadata,
    df_threshold_metadata,
    df_carveout_metadata,
    df_var_mapping,
    df_carveout_data
):
    log.info(f"Fetching metadata for ALERT_TYPE_ID : {alert_type_id}")

    # 1. Convert metadata rows into Python dictionaries
    alert_metadata = df_alert_metadata.first().asDict()

    threshold_metadata = df_threshold_metadata.collect()
    carveout_metadata = df_carveout_metadata.collect()
    var_mapping = df_var_mapping.collect()
    carveout_data = df_carveout_data.collect()

    log.info("Metadata queries completed. Starting transformation.")

    # 2. Threshold parsing
    thresholds = defaultdict(dict)
    for row in threshold_metadata:
        name = row["TH_NAME"]
        category = row["CATEGORY"]
        if category == 'TH':
            thresholds[name]["clb_phy_column_name"] = row["CLB_PHY_COLUMN_NAME"]
            thresholds[name]["th_logic"] = row["TH_LOGIC"]
        elif category == 'TH.CALC':
            thresholds[name]["th_calc"] = row["CLB_PHY_COLUMN_NAME"]
        elif category == 'FILTER':
            thresholds[name]["th_condition"] = row["TH_CONDITION"]

    # 3. Carveout handling
    carveout_names = [row["TH_NAME"] for row in carveout_metadata]
    carveout_clb_names = [row["CLB_PHY_COLUMN_NAME"] for row in carveout_metadata]
    carveout_column_map = {row["TH_NAME"]: row["CLB_PHY_COLUMN_NAME"] for row in carveout_metadata}

    display_to_var = {
        normalize_key(row["DISPLAYNAME"]): normalize_key(row["FIELDNAME"])
        for row in var_mapping
    }

    carveout_var_fields = {
        row["CLB_PHY_COLUMN_NAME"]: try_mapping(display_to_var, row["TH_NAME"])
        for row in carveout_metadata
    }

    threshold_var_field = try_mapping(display_to_var, 'Name')

    # 4. Carveout combination building
    carveout_combinations_by_threshold = defaultdict(list)
    for row in carveout_data:
        actual_keys = {normalize_key(k): k for k in row.asDict()}
        threshold = row.asDict().get(actual_keys.get(threshold_var_field.lower()))
        if not threshold:
            continue

        carveout_values = {}
        for name in carveout_names:
            column = carveout_column_map[name]
            mapped_var = carveout_var_fields.get(column)
            key = actual_keys.get(mapped_var.lower())
            if key:
                carveout_values[column] = row[key]
        carveout_combinations_by_threshold[threshold].append(carveout_values)

    # 5. Final Row Assembly
    rows = []
    for th_name in thresholds:
        th_info = thresholds[th_name]
        carveout_rows = carveout_combinations_by_threshold.get(th_name, [{}])
        for carveout_vals in carveout_rows:
            row = {
                'alert_type_id': alert_type_id,
                'alert_type': alert_metadata['ALERT_TYPE'],
                'alert_type_value': alert_metadata['ALERT_TYPE_VALUE'],
                'impala_table_name': alert_metadata['IMPALA_TABLE_NAME'],
                'clb_file_path': alert_metadata['CLB_FILE_PATH'],
                'th_name': th_name,
                'clb_phy_column_name': th_info.get('clb_phy_column_name'),
                'th_logic': th_info.get('th_logic'),
                'th_calc': th_info.get('th_calc'),
                'th_condition': th_info.get('th_condition')
            }
            row.update(carveout_vals)
            rows.append(Row(**row))

    # 6. Convert to Spark DataFrame
    if rows:
        df_final = df_alert_metadata.sql_ctx.createDataFrame(rows)
        log.info(f"Metadata transformation complete. Final count: {df_final.count()}")
    else:
        df_final = df_alert_metadata.sql_ctx.createDataFrame([], df_alert_metadata.schema)
        log.warning("No rows generated in final DataFrame.")

    return (
        df_final,
        alert_metadata['CLB_FILE_PATH'],
        alert_metadata['ALERT_TYPE'],
        alert_metadata['ALERT_TYPE_VALUE'],
        carveout_clb_names
    )
