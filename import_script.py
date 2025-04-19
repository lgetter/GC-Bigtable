import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\\Users\\lukas\\Projects\\Bigtable\\concise-flame-450703-h0-15e283dc99c6.json"

import pandas as pd
from google.cloud import bigtable
from google.cloud.bigtable.row import DirectRow
from google.cloud.bigtable.row_filters import RowKeyRegexFilter

project_id = "concise-flame-450703-h0"
instance_id = "ev-bigtable"
table_id = "ev-population"
column_family_id = "ev_info"
csv_path = "./Electric_Vehicle_Population_Data.csv"

columns = [
    "DOL Vehicle ID",
    "Make",
    "Model",
    "Model Year",
    "Electric Range",
    "City",
    "County"
]

df = pd.read_csv(csv_path, dtype=str, usecols=columns).fillna("")

client = bigtable.Client(project=project_id, admin=True)
table = client.instance(instance_id).table(table_id)

for index, row in df.iterrows():
    row_key = row["DOL Vehicle ID"]

    row_filter = RowKeyRegexFilter(row_key.encode("utf-8"))
    rows = table.read_rows(filter_=row_filter)

    row_exists = False
    for r in rows:
        if r.row_key == row_key.encode("utf-8"):
            row_exists = True
            break

    if not row_exists:
        bt_row = DirectRow(row_key.encode("utf-8"), table=table)

        for col in columns:
            if col != "DOL Vehicle ID":
                qualifier = col.strip().replace(" ", "_").lower()
                value = row[col]
                bt_row.set_cell(column_family_id, qualifier.encode("utf-8"), value.encode("utf-8"))

        bt_row.commit()
        print(f"Uploaded row with key: {row_key}")