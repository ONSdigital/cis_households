from cishouseholds import config


def update_table(df, table_name):
    storage_config = config["storage"]
    df.write.mode(storage_config["write_mode"]).saveAsTable(
        f"{storage_config['database']}.{storage_config['table_prefix']}{table_name}"
    )
