"""
BigQuery schema management functionality.
"""
import json
from typing import Dict, List, Optional, Any, Tuple

from google.cloud import bigquery
from rich.console import Console
from rich.table import Table
from rich.text import Text
from rich import box
import difflib

from bench.utils import get_client

console = Console()

def get_schema(
    table_id: str,
    project_id: Optional[str] = None,
    output_format: str = "table",
    detailed: bool = False
) -> Dict[str, Any]:
    """
    Get and display the schema of a BigQuery table.
    
    Args:
        table_id: Table ID in the format "dataset.table" or "project.dataset.table"
        project_id: GCP project ID (optional if included in table_id)
        output_format: Output format (table or json)
        detailed: Whether to include detailed field information
    
    Returns:
        Dict containing the schema information
    """
    try:
        client = get_client(project_id)
        
        # Parse the table reference
        table_ref_parts = table_id.split(".")
        
        if len(table_ref_parts) == 3:
            # project.dataset.table format
            table_ref = client.dataset(table_ref_parts[1], project=table_ref_parts[0]).table(table_ref_parts[2])
        elif len(table_ref_parts) == 2:
            # dataset.table format
            table_ref = client.dataset(table_ref_parts[0]).table(table_ref_parts[1])
        else:
            return {"error": "Invalid table_id format. Use 'dataset.table' or 'project.dataset.table'."}
        
        # Get the table
        table = client.get_table(table_ref)
        
        # Convert schema to a list of dicts
        schema_fields = []
        for field in table.schema:
            field_info = {
                "name": field.name,
                "type": field.field_type,
                "mode": field.mode,
            }
            
            if detailed:
                field_info.update({
                    "description": field.description,
                    "fields": _extract_nested_fields(field) if field.fields else None
                })
            
            schema_fields.append(field_info)
        
        # Output based on format
        if output_format == "json":
            result = {
                "table_id": table.full_table_id,
                "created": table.created.isoformat() if table.created else None,
                "modified": table.modified.isoformat() if table.modified else None,
                "num_rows": table.num_rows,
                "num_bytes": table.num_bytes,
                "schema": schema_fields
            }
            console.print(json.dumps(result, indent=2))
            return result
        else:
            # Create a rich table
            schema_table = Table(
                title=f"Schema for {table.full_table_id}",
                box=box.SIMPLE_HEAD
            )
            
            # Add columns
            schema_table.add_column("Name", style="cyan")
            schema_table.add_column("Type", style="green")
            schema_table.add_column("Mode", style="yellow")
            if detailed:
                schema_table.add_column("Description", style="dim")
            
            # Add rows
            for field in schema_fields:
                row = [
                    field["name"],
                    field["type"],
                    field["mode"] or "NULLABLE",
                ]
                if detailed:
                    row.append(field["description"] or "")
                
                schema_table.add_row(*row)
            
            # Display the table
            console.print(schema_table)
            
            # Show metadata
            console.print(f"[bold]Table Info:[/]")
            console.print(f"  Created: {table.created.isoformat() if table.created else 'Unknown'}")
            console.print(f"  Last modified: {table.modified.isoformat() if table.modified else 'Unknown'}")
            console.print(f"  Rows: {table.num_rows or 0:,}")
            console.print(f"  Size: {table.num_bytes or 0:,} bytes")
            
            return {
                "table_id": table.full_table_id,
                "created": table.created.isoformat() if table.created else None,
                "modified": table.modified.isoformat() if table.modified else None,
                "num_rows": table.num_rows,
                "num_bytes": table.num_bytes,
                "schema": schema_fields
            }
            
    except Exception as e:
        return {"error": str(e)}


def diff_schemas(
    table1_id: str,
    table2_id: str,
    project_id: Optional[str] = None,
    output_format: str = "rich"
) -> Dict[str, Any]:
    """
    Compare schemas between two BigQuery tables.
    
    Args:
        table1_id: First table ID
        table2_id: Second table ID
        project_id: GCP project ID (optional)
        output_format: Output format (rich, text, or json)
    
    Returns:
        Dict containing the schema differences
    """
    try:
        # Get both schemas
        schema1_result = get_schema(table1_id, project_id, "json", True)
        schema2_result = get_schema(table2_id, project_id, "json", True)
        
        if schema1_result.get("error"):
            return {"error": f"Error getting schema for {table1_id}: {schema1_result['error']}"}
        
        if schema2_result.get("error"):
            return {"error": f"Error getting schema for {table2_id}: {schema2_result['error']}"}
        
        schema1 = schema1_result["schema"]
        schema2 = schema2_result["schema"]
        
        # Compare field names, types, and modes
        schema1_dict = {field["name"]: field for field in schema1}
        schema2_dict = {field["name"]: field for field in schema2}
        
        all_fields = sorted(set(list(schema1_dict.keys()) + list(schema2_dict.keys())))
        
        # Organize differences
        added = [field for field in all_fields if field in schema2_dict and field not in schema1_dict]
        removed = [field for field in all_fields if field in schema1_dict and field not in schema2_dict]
        changed = [field for field in all_fields 
                  if field in schema1_dict and field in schema2_dict and 
                  (schema1_dict[field]["type"] != schema2_dict[field]["type"] or
                   schema1_dict[field]["mode"] != schema2_dict[field]["mode"])]
        unchanged = [field for field in all_fields 
                    if field in schema1_dict and field in schema2_dict and
                    schema1_dict[field]["type"] == schema2_dict[field]["type"] and
                    schema1_dict[field]["mode"] == schema2_dict[field]["mode"]]
        
        # Generate change details
        changes = []
        for field in changed:
            changes.append({
                "field": field,
                "from": {
                    "type": schema1_dict[field]["type"],
                    "mode": schema1_dict[field]["mode"]
                },
                "to": {
                    "type": schema2_dict[field]["type"],
                    "mode": schema2_dict[field]["mode"]
                }
            })
        
        # Build the result
        diff_result = {
            "table1": table1_id,
            "table2": table2_id,
            "added": [{"field": field, "details": schema2_dict[field]} for field in added],
            "removed": [{"field": field, "details": schema1_dict[field]} for field in removed],
            "changed": changes,
            "unchanged": len(unchanged)
        }
        
        # Output based on format
        if output_format == "json":
            console.print(json.dumps(diff_result, indent=2))
            return diff_result
        
        elif output_format == "text":
            # Simple text diff
            diff_lines = []
            diff_lines.append(f"Schema diff: {table1_id} → {table2_id}")
            diff_lines.append(f"Added fields: {len(added)}")
            diff_lines.append(f"Removed fields: {len(removed)}")
            diff_lines.append(f"Changed fields: {len(changed)}")
            diff_lines.append(f"Unchanged fields: {len(unchanged)}")
            
            if added:
                diff_lines.append("\nAdded fields:")
                for item in diff_result["added"]:
                    field = item["details"]
                    diff_lines.append(f"+ {field['name']} ({field['type']}, {field['mode']})")
            
            if removed:
                diff_lines.append("\nRemoved fields:")
                for item in diff_result["removed"]:
                    field = item["details"]
                    diff_lines.append(f"- {field['name']} ({field['type']}, {field['mode']})")
            
            if changed:
                diff_lines.append("\nChanged fields:")
                for change in diff_result["changed"]:
                    diff_lines.append(
                        f"~ {change['field']}: "
                        f"({change['from']['type']}, {change['from']['mode']}) → "
                        f"({change['to']['type']}, {change['to']['mode']})"
                    )
            
            print("\n".join(diff_lines))
            return diff_result
        
        else:  # rich format
            # Create tables for each category
            console.print(f"[bold]Schema diff:[/] [cyan]{table1_id}[/] → [cyan]{table2_id}[/]")
            
            summary = Table(show_header=False, box=box.SIMPLE)
            summary.add_column("Category")
            summary.add_column("Count")
            
            summary.add_row("Added fields", f"[green]{len(added)}[/]")
            summary.add_row("Removed fields", f"[red]{len(removed)}[/]")
            summary.add_row("Changed fields", f"[yellow]{len(changed)}[/]")
            summary.add_row("Unchanged fields", f"[dim]{len(unchanged)}[/]")
            
            console.print(summary)
            
            # Added fields
            if added:
                added_table = Table(title="Added Fields", box=box.SIMPLE_HEAD)
                added_table.add_column("Field Name", style="green")
                added_table.add_column("Type", style="cyan")
                added_table.add_column("Mode", style="yellow")
                
                for field in added:
                    added_table.add_row(
                        field,
                        schema2_dict[field]["type"],
                        schema2_dict[field]["mode"] or "NULLABLE"
                    )
                
                console.print(added_table)
            
            # Removed fields
            if removed:
                removed_table = Table(title="Removed Fields", box=box.SIMPLE_HEAD)
                removed_table.add_column("Field Name", style="red")
                removed_table.add_column("Type", style="cyan")
                removed_table.add_column("Mode", style="yellow")
                
                for field in removed:
                    removed_table.add_row(
                        field,
                        schema1_dict[field]["type"],
                        schema1_dict[field]["mode"] or "NULLABLE"
                    )
                
                console.print(removed_table)
            
            # Changed fields
            if changed:
                changed_table = Table(title="Changed Fields", box=box.SIMPLE_HEAD)
                changed_table.add_column("Field Name", style="yellow")
                changed_table.add_column("From Type", style="cyan")
                changed_table.add_column("To Type", style="cyan")
                changed_table.add_column("From Mode", style="dim")
                changed_table.add_column("To Mode", style="dim")
                
                for field in changed:
                    changed_table.add_row(
                        field,
                        schema1_dict[field]["type"],
                        schema2_dict[field]["type"],
                        schema1_dict[field]["mode"] or "NULLABLE",
                        schema2_dict[field]["mode"] or "NULLABLE"
                    )
                
                console.print(changed_table)
            
            return diff_result
            
    except Exception as e:
        return {"error": str(e)}


def _extract_nested_fields(field: bigquery.SchemaField, prefix: str = "") -> List[Dict[str, Any]]:
    """
    Extract nested fields from a BigQuery schema field.
    
    Args:
        field: BigQuery SchemaField object
        prefix: Prefix for nested field names
    
    Returns:
        List of field dictionaries
    """
    result = []
    
    if not field.fields:
        return result
    
    for nested_field in field.fields:
        field_info = {
            "name": f"{prefix}{nested_field.name}",
            "type": nested_field.field_type,
            "mode": nested_field.mode,
            "description": nested_field.description
        }
        
        result.append(field_info)
        
        if nested_field.fields:
            nested = _extract_nested_fields(
                nested_field, 
                prefix=f"{prefix}{nested_field.name}."
            )
            result.extend(nested)
    
    return result
