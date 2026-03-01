import os
from datetime import datetime
from typing import Dict, Any
from pydantic import BaseModel
from fastapi import APIRouter, Request, HTTPException, Depends
from fastapi.responses import JSONResponse

from src.services.bq_metadata import BigQueryMetadata
from src.services.bq_metadata_uploader import BigQueryMetadataUploader
from src.database.db_config import GoogleCloudSqlUtility
from src.utils.mock_ldap import verify_token
from src.utils.logger import logger

class UploadBqMetadataRequest(BaseModel):
    project_id: str
    db_config: Dict[str, Any] = None


"""
Add these three endpoints at the end of your routes.py file
"""

@router.post("/upload_bq_metadata")
async def upload_bq_metadata(request: UploadBqMetadataRequest, user: dict = Depends(verify_token)):
    """Extract BigQuery metadata and upload to PostgreSQL"""
    username = user.get('username', 'unknown')
    logger.info("UPLOAD_BQ_METADATA started - User: %s, Project: %s", username, request.project_id)
    
    if os.getenv("TEST_MODE") == "true":
        return JSONResponse(content={"message": "Mocked response in test mode"}, status_code=200)
    
    try:
        start_time = datetime.now()
        
        # Get DB config
        if request.db_config:
            db_config = request.db_config
        else:
            db_config = {
                "host": os.getenv("POSTGRES_HOST", "localhost"),
                "port": int(os.getenv("POSTGRES_PORT", "5432")),
                "user": os.getenv("POSTGRES_USER", "postgres"),
                "password": os.getenv("POSTGRES_PASSWORD", ""),
                "database": os.getenv("POSTGRES_DATABASE", "postgres")
            }
        
        # Extract metadata
        extractor = BigQueryMetadataExtractor(request.project_id)
        datasets_metadata = extractor.extract_all_metadata()
        
        # Calculate extraction stats
        extraction_stats = {
            'total_datasets': len(datasets_metadata),
            'total_tables': sum(d['statistics']['total_tables'] for d in datasets_metadata),
            'total_columns': sum(d['statistics']['total_columns'] for d in datasets_metadata),
            'partitioned_tables': sum(d['statistics']['partitioned_tables'] for d in datasets_metadata),
            'clustered_tables': sum(d['statistics']['clustered_tables'] for d in datasets_metadata),
            'total_size_mb': sum(d['statistics']['total_size_mb'] for d in datasets_metadata),
        }
        
        # Upload to PostgreSQL
        uploader = BigQueryMetadataUploader(db_config)
        upload_stats = uploader.upload(datasets_metadata)
        
        duration = (datetime.now() - start_time).total_seconds()
        logger.info("UPLOAD_BQ_METADATA completed - User: %s, Duration: %.2fs", username, duration)
        
        return JSONResponse(
            status_code=200,
            content={
                'success': True,
                'message': 'Metadata extraction and upload completed successfully',
                'project_id': request.project_id,
                'extraction_stats': extraction_stats,
                'upload_stats': upload_stats,
                'duration_seconds': round(duration, 2)
            }
        )
        
    except Exception as e:
        logger.exception("UPLOAD_BQ_METADATA_ERROR - User: %s, Error: %s", username, str(e))
        raise HTTPException(status_code=500, detail=str(e))

