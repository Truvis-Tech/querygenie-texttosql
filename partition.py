from google.cloud import bigquery

def get_partition_info(project_id: str, dataset_id: str, table_id: str):
    client = bigquery.Client(project=project_id)

    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    table = client.get_table(table_ref)

    print(f"\nTable: {table_ref}")
    print(f"Table Type: {table.table_type}")

    # Time-based partition info
    time_partitioning = table.time_partitioning
    if time_partitioning:
        print("\n✅ Time Partitioning Found")
        if time_partitioning.field:
            print(f"Partition Field: {time_partitioning.field}")
        else:
            print("Partition Type: Ingestion-time partitioning (_PARTITIONTIME)")
        print(f"Partition Type: {time_partitioning.type_}")
        if time_partitioning.expiration_ms:
            print(f"Expiration (ms): {time_partitioning.expiration_ms}")
    else:
        print("\n❌ No time partitioning configured.")

    # Range partition info (integer range partitioning)
    range_partitioning = table.range_partitioning
    if range_partitioning:
        print("\n✅ Range Partitioning Found")
        print(f"Partition Field: {range_partitioning.field}")
        print(f"Range Start: {range_partitioning.range_.start}")
        print(f"Range End: {range_partitioning.range_.end}")
        print(f"Range Interval: {range_partitioning.range_.interval}")
    else:
        print("\n❌ No range partitioning configured.")


if __name__ == "__main__":
    project_id = "your-gcp-project-id"
    dataset_id = "your_dataset"
    table_id = "your_table"

    get_partition_info(project_id, dataset_id, table_id)
















from google.cloud import bigquery

def get_partition_field_via_information_schema(project_id: str, dataset_id: str, table_id: str):
    client = bigquery.Client(project=project_id)

    query = f"""
    SELECT
      table_name,
      partitioning_type,
      partitioning_field
    FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = '{table_id}'
    """

    result = client.query(query).result()

    rows = list(result)
    if not rows:
        print("❌ Table not found.")
        return

    row = rows[0]
    print(f"\nTable: {row.table_name}")
    print(f"Partitioning Type: {row.partitioning_type}")

    if row.partitioning_field:
        print(f"✅ Partition Field: {row.partitioning_field}")
    elif row.partitioning_type:
        print("✅ Ingestion-time partitioned table (_PARTITIONTIME)")
    else:
        print("❌ Not partitioned.")


if __name__ == "__main__":
    project_id = "your-gcp-project-id"
    dataset_id = "your_dataset"
    table_id = "your_table"

    get_partition_field_via_information_schema(project_id, dataset_id, table_id)
