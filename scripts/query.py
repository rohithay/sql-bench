"""
Query execution and dry run functionality.
"""
import time
from typing import Dict, Optional, Any

from google.cloud import bigquery
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
import json
import tabulate

from bench.utils import get_client, format_bytes, estimate_cost

# Setup console for pretty output
console = Console()

def run_query(
    sql: str,
    project_id: Optional[str] = None,
    dataset_id: Optional[str] = None,
    limit: int = 10,
    output_format: str = "table"
) -> Dict[str, Any]:
    """
    Run a BigQuery query and return the results.
    
    Args:
        sql: The SQL query to run
        project_id: GCP project ID (optional)
        dataset_id: BigQuery dataset ID (optional)
        limit: Maximum number of results to return
        output_format: Output format (table, json, or csv)
        
    Returns:
        Dict containing results and metadata
    """
    try:
        client = get_client(project_id)
        
        # Construct the job config
        job_config = bigquery.QueryJobConfig()
        if dataset_id:
            job_config.default_dataset = f"{project_id}.{dataset_id}" if project_id else dataset_id
        
        # Start timer
        start_time = time.time()
        
        # Run the query
        query_job = client.query(sql, job_config=job_config)
        results = query_job.result()  # Waits for query to finish
        
        # Calculate execution time
        execution_time = time.time() - start_time
        
        # Get the schema and convert to list of dicts for easier handling
        rows = list(results)
        if not rows:
            console.print("[yellow]Query executed successfully but returned no results.[/]")
            console.print(f"[green]✓[/] Execution time: {execution_time:.2f} seconds")
            console.print(f"[green]✓[/] Bytes processed: {format_bytes(query_job.total_bytes_processed)}")
            console.print(f"[green]✓[/] Estimated cost: ${estimate_cost(query_job.total_bytes_processed):.5f}")
            return {
                "success": True,
                "execution_time": execution_time,
                "bytes_processed": query_job.total_bytes_processed,
                "rows_returned": 0,
                "results": []
            }
        
        # Output the results based on the format
        if output_format == "json":
            # Convert to list of dicts
            output = []
            for row in rows[:limit]:
                item = {}
                for key, value in row.items():
                    item[key] = value
                output.append(item)
            
            console.print(json.dumps(output, indent=2, default=str))
            
        elif output_format == "csv":
            headers = [field.name for field in results.schema]
            data = [[row[header] for header in headers] for row in rows[:limit]]
            print(tabulate.tabulate(data, headers=headers, tablefmt="csv"))
            
        else:  # table format (default)
            table = Table(title=f"Query Results (showing {min(limit, len(rows))} of {len(rows)} rows)")
            
            # Add columns
            for field in results.schema:
                table.add_column(field.name, style="cyan")
            
            # Add rows
            for row in rows[:limit]:
                table.add_row(*[str(row[field.name]) for field in results.schema])
            
            console.print(table)
        
        # Print summary
        console.print(f"[green]✓[/] Execution time: {execution_time:.2f} seconds")
        console.print(f"[green]✓[/] Bytes processed: {format_bytes(query_job.total_bytes_processed)}")
        console.print(f"[green]✓[/] Estimated cost: ${estimate_cost(query_job.total_bytes_processed):.5f}")
        console.print(f"[green]✓[/] Rows returned: {len(rows)}")
        
        return {
            "success": True,
            "execution_time": execution_time,
            "bytes_processed": query_job.total_bytes_processed,
            "rows_returned": len(rows),
            "results": rows[:limit]
        }
        
    except Exception as e:
        return {"error": str(e)}


def dry_run_query(
    sql: str,
    project_id: Optional[str] = None,
    dataset_id: Optional[str] = None,
    output_format: str = "table"
) -> Dict[str, Any]:
    """
    Perform a dry run of a BigQuery query to validate it and estimate cost.
    
    Args:
        sql: The SQL query to validate
        project_id: GCP project ID (optional)
        dataset_id: BigQuery dataset ID (optional)
        output_format: Output format (table or json)
        
    Returns:
        Dict containing validation result and bytes to be processed
    """
    try:
        client = get_client(project_id)
        
        # Construct the job config
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        if dataset_id:
            job_config.default_dataset = f"{project_id}.{dataset_id}" if project_id else dataset_id
        
        # Run the dry run
        query_job = client.query(sql, job_config=job_config)
        
        # Get bytes to be processed
        bytes_to_be_processed = query_job.total_bytes_processed
        estimated_cost = estimate_cost(bytes_to_be_processed)
        
        # Success - the query is valid
        if output_format == "json":
            result = {
                "valid": True,
                "bytes_to_be_processed": bytes_to_be_processed,
                "estimated_cost": estimated_cost
            }
            console.print(json.dumps(result, indent=2))
        else:
            panel = Panel(
                f"[green]✓ Query valid[/]\n"
                f"Bytes to be processed: [yellow]{format_bytes(bytes_to_be_processed)}[/]\n"
                f"Estimated cost: [yellow]${estimated_cost:.5f}[/]\n\n"
                f"[dim]Run with: bench run \"SELECT ...\" to execute this query[/]",
                title="Dry Run Results",
                border_style="green"
            )
            console.print(panel)
        
        return {
            "valid": True,
            "bytes_to_be_processed": bytes_to_be_processed,
            "estimated_cost": estimated_cost
        }
        
    except Exception as e:
        if output_format == "json":
            result = {
                "valid": False,
                "error": str(e)
            }
            console.print(json.dumps(result, indent=2))
        else:
            panel = Panel(
                f"[red]✗ Query invalid[/]\n"
                f"Error: {str(e)}",
                title="Dry Run Results",
                border_style="red"
            )
            console.print(panel)
            
        return {
            "valid": False,
            "error": str(e)
        }
