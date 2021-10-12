from cishouseholds import config


def update_table(df, table_name):
    df.write.mode("append").saveAsTable(
        f"{config['storage']['database']}.{config['storage']['table_prefix']}{table_name}"
    )
