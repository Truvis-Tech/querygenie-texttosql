from typing import Tuple, Optional, Dict, Any
from datetime import datetime
from src.database.postgres_loader import PostgresLoader
from src.utils.hash_audit import compute_md5_hash
from src.utils.logger import logger

def serialize_datetime_values(data: Dict[str, Any]) -> Dict[str, Any]:
    """Convert datetime objects to ISO format strings for JSON serialization"""
    if not data:
        return data
        
    serialized_data = {}
    for key, value in data.items():
        if isinstance(value, datetime):
            serialized_data[key] = value.isoformat()
        else:
            serialized_data[key] = value
    return serialized_data

def handle_generic_audit(
    pg: PostgresLoader,
    cur,
    user_id: str,
    user_name: str,
    old_data: Dict[str, Any],
    new_data: Dict[str, Any],
    entity_name: str,
    entity_id: str
):
    logger.debug(f"Handling audit for entity: {entity_name}, ID: {entity_id}, User: {user_name}")
    
    try:
        action, old_changes, new_changes, metadata = determine_audit_action(old_data, new_data)
        
        if action:
            logger.info(f"Audit action detected: {action} for {entity_name}:{entity_id}")
            logger.debug(f"Changes - Old: {len(old_changes)} fields, New: {len(new_changes)} fields")
            
            serialized_old_changes = serialize_datetime_values(old_changes)
            serialized_new_changes = serialize_datetime_values(new_changes)
            
            pg.insert_audit_log(
                cur=cur,
                user_id=user_id,
                user_name=user_name,
                action=action,
                entity_name=entity_name,
                entity_id=entity_id,
                old_data=serialized_old_changes,
                new_data=serialized_new_changes,
                metadata=metadata
            )
            logger.info(f"Audit log inserted successfully for {entity_name}:{entity_id}")
        else:
            logger.debug(f"No audit action required for {entity_name}:{entity_id}")
            
    except Exception as e:
        logger.error(f"Audit handling failed for {entity_name}:{entity_id}: {str(e)}", exc_info=True)
        raise

def get_table_audit_data(cur, id_key: str):
    logger.debug(f"Getting table audit data for id_key: {id_key}")
    
    try:
        cur.execute("SELECT * FROM table_config WHERE id_key = %s", (id_key,))
        columns = [desc[0] for desc in cur.description]
        row = cur.fetchone()
        
        if row:
            result = dict(zip(columns, row))
            logger.debug(f"Table audit data found for {id_key}: {len(result)} fields")
            return result
        else:
            logger.warning(f"No table audit data found for id_key: {id_key}")
            return {}
            
    except Exception as e:
        logger.error(f"Failed to get table audit data for {id_key}: {str(e)}", exc_info=True)
        return {}

def get_column_audit_data(cur, id_key: str):
    logger.debug(f"Getting column audit data for id_key: {id_key}")
    
    try:
        cur.execute("SELECT * FROM column_config WHERE id_key = %s", (id_key,))
        columns = [desc[0] for desc in cur.description]
        row = cur.fetchone()
        
        if row:
            result = dict(zip(columns, row))
            logger.debug(f"Column audit data found for {id_key}: {len(result)} fields")
            return result
        else:
            logger.warning(f"No column audit data found for id_key: {id_key}")
            return {}
            
    except Exception as e:
        logger.error(f"Failed to get column audit data for {id_key}: {str(e)}", exc_info=True)
        return {}

def determine_audit_action(old_data: dict, new_data: dict) -> Tuple[Optional[str], Dict[str, Any], Dict[str, Any], dict]:
    logger.debug("Determining audit action")
    FIELDS_TO_IGNORE = {'created_at', 'updated_at', 'created_by', 'updated_by'}

    try:
        if not old_data:
            logger.debug("No old data to compare against, treating as CREATE. No audit action needed here.")
            return None, {}, {}, {}

        old_comparable = {k: v for k, v in old_data.items() if k not in FIELDS_TO_IGNORE}
        new_comparable = {k: v for k, v in new_data.items() if k not in FIELDS_TO_IGNORE}
        
        logger.debug(f"Old comparable fields: {len(old_comparable)}, New comparable fields: {len(new_comparable)}")
        serialized_old_data = serialize_datetime_values(old_comparable)
        serialized_new_data = serialize_datetime_values(new_comparable)
        
        old_hash = compute_md5_hash(serialized_old_data)
        new_hash = compute_md5_hash(serialized_new_data)
        logger.debug(f"Data hashes - Old: {old_hash[:8]}..., New: {new_hash[:8]}...")

        if old_hash != new_hash:
            is_delete = not new_comparable or all(v is None for v in new_comparable.values())
            action = "DELETE" if is_delete else "UPDATE"
            
            logger.info(f"Data change detected: {action}")
            
            comparable_keys = set(old_comparable.keys()) | set(new_comparable.keys())
            changed_fields = {}
            
            for k in comparable_keys:
                old_val = old_comparable.get(k)
                new_val = new_comparable.get(k)
                
                if isinstance(old_val, datetime) and isinstance(new_val, datetime):
                    if old_val != new_val:
                        changed_fields[k] = {"old": old_val, "new": new_val}
                elif old_val != new_val:
                    changed_fields[k] = {"old": old_val, "new": new_val}
            
            old_changes = {k: v["old"] for k, v in changed_fields.items()}
            new_changes = {k: v["new"] for k, v in changed_fields.items()}
            
            logger.debug(f"Changed fields: {list(changed_fields.keys())}")
            
            return action, old_changes, new_changes, {}
        else:
            logger.debug("No data changes detected")
            return None, {}, {}, {}
            
    except Exception as e:
        logger.error(f"Failed to determine audit action: {str(e)}", exc_info=True)
        return None, {}, {}, {}