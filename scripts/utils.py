"""
Utility functions for bench.
"""
import os
from typing import Optional

from google.cloud import bigquery
from google.oauth2 import service_account

def get_client(project_id: Optional[str] = None) -> bigquery.Client:
    """
    Get a BigQuery client with the specified project_id.
    
    Args:
        project_id: GCP project ID
    
    Returns:
        BigQuery client
    """
    # Check for service account key path in environment
    creds_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    
    if creds_path and os.path.exists(creds_path):
        credentials = service_account.Credentials.from_service_account_file(creds_path)
        if project_id:
            client = bigquery.Client(credentials=credentials, project=project_id)
        else:
            client = bigquery.Client(credentials=credentials)
    else:
        # Use default credentials
        if project_id:
            client = bigquery.Client(project=project_id)
        else:
            client = bigquery.Client()
    
    return client


def format_bytes(byte_count: int) -> str:
    """
    Format byte count into human-readable string.
    
    Args:
        byte_count: Number of bytes
    
    Returns:
        Formatted string (e.g., "1.23 GB")
    """
    if byte_count == 0:
        return "0 B"
    
    size_names = ("B", "KB", "MB", "GB", "TB", "PB")
    i = 0
    while byte_count >= 1024 and i < len(size_names) - 1:
        byte_count /= 1024
        i += 1
    
    return f"{byte_count:.2f} {size_names[i]}"


def estimate_cost(bytes_processed: int) -> float:
    """
    Estimate the cost of a query based on bytes processed.
    Uses a default rate of $5 per TB.
    
    Args:
        bytes_processed: Number of bytes processed by the query
    
    Returns:
        Estimated cost in USD
    """
    # Default rate: $5 per TB
    # 1 TB = 1,099,511,627,776 bytes
    return (bytes_processed / 1099511627776) * 5


def parse_table_id(table_id: str, project_id: Optional[str] = None) -> tuple:
    """
    Parse a table ID into its components.
    
    Args:
        table_id: Table ID in the format "dataset.table" or "project.dataset.table"
        project_id: Default project ID to use if not specified in table_id
    
    Returns:
        Tuple of (project_id, dataset_id, table_id)
    """
    parts = table_id.split(".")
    
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    elif len(parts) == 2:
        if not project_id:
            raise ValueError("Project ID is required when table_id is in 'dataset.table' format")
        return project_id, parts[0], parts[1]
    else:
        raise ValueError("Invalid table_id format. Use 'dataset.table' or 'project.dataset.table'.")
