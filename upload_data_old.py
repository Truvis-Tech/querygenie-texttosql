import json
import psycopg2

# --- 1. Database Connection Parameters ---
# The database name is set to 'postgres' based on your pgAdmin image.
# Replace the user, password, host, and port with your specific details.
db_params = {
    "host": "",
    "port": 5432,
    "user": "postgres",
    "password": "",
    "database": "postgres"
}

# --- 2. Load JSON Data ---
# Create a file named `data.json` in the same directory as this script
# and paste your JSON content into it.
try:
    with open('static.json', 'r') as f:
        json_data = json.load(f)
except FileNotFoundError:
    print("Error: 'data.json' not found. Please create this file and add your JSON data to it.")
    exit()

# --- 3. SQL Statements ---
# SQL statement to create the 'context' table inside the 'public' schema.
create_table_query = """
CREATE TABLE IF NOT EXISTS public.context (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    table_description TEXT,
    schema_details JSONB,
    num_rows INTEGER,
    size_bytes BIGINT,
    last_modified TIMESTAMP,
    created TIMESTAMP,
    partitioning_info JSONB,
    clustering_fields JSONB
);
"""

# SQL statement to insert records into the 'context' table.
insert_query = """
INSERT INTO public.context (
    table_name, table_description, schema_details, num_rows, size_bytes,
    last_modified, created, partitioning_info, clustering_fields
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

# --- 4. Execution ---
try:
    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # Execute the command to create the table
    cur.execute(create_table_query)
    print("Table 'public.context' created successfully or already exists.")

    # Loop through each JSON object and insert it as a new row
    for record in json_data:
        cur.execute(insert_query, (
            record.get("table_name"),
            record.get("table_description"),
            json.dumps(record.get("schema")),
            record.get("num_rows"),
            record.get("size_bytes"),
            record.get("last_modified"),
            record.get("created"),
            json.dumps(record.get("partitioning_info")),
            json.dumps(record.get("clustering_fields"))
        ))
    
    # Commit the changes to the database
    conn.commit()
    print(f"{len(json_data)} records were inserted successfully into 'public.context'.")

except psycopg2.Error as e:
    print(f"A database error occurred: {e}")

finally:
    # Cleanly close the database connection
    if 'cur' in locals() and cur:
        cur.close()
    if 'conn' in locals() and conn:
        conn.close()
        print("Database connection has been closed.")
