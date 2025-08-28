import json
import os
import json
from src.services.bq_metadata_extractor import BigQueryMetadataExtractor
from src.services.bq_client import get_bigquery_client

def main():
    """
    Main function to extract BigQuery metadata and save it to static.json
    """
    try:
        # Initialize BigQuery client using the project's bq_client
        client, project_id, dataset_id, location = get_bigquery_client('linen-striker-454116-c9')
        
        print(f"Connected to BigQuery project: {project_id}")
        print(f"Using dataset: {dataset_id} in location: {location}")
        
        # Initialize the metadata extractor
        extractor = BigQueryMetadataExtractor(client)
        
        print(f"Extracting metadata for dataset: {dataset_id}")
        
        # Get all tables metadata
        metadata = extractor.get_all_tables_metadata(dataset_id)
        
        # Format the output as a list of table metadata dictionaries
        tables_metadata = metadata.get('tables', [])
        
        # Save to static.json
        output_file = 'static.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(tables_metadata, f, indent=2, default=str)
            
        print(f"Successfully saved {len(tables_metadata)} tables' metadata to {output_file}")
        if 'summary' in metadata:
            print(f"Summary: {metadata['summary']}")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()
