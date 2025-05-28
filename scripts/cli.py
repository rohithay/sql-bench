"""
Command-line interface for bench.
"""
import sys
from typing import Optional

import click
from rich.console import Console

from bench import __version__
from bench.query import run_query, dry_run_query
from bench.schema import get_schema, diff_schemas
from bench.lint import lint_sql

# Setup console for pretty output
console = Console()

@click.group()
@click.version_option(version=__version__)
def cli():
    """A minimal BigQuery utility toolkit."""
    pass


@cli.command()
@click.argument("sql", required=False)
@click.option("--file", "-f", help="SQL file path")
@click.option("--project", "-p", help="GCP project ID")
@click.option("--dataset", "-d", help="BigQuery dataset")
@click.option("--limit", "-l", type=int, default=10, help="Limit the number of results")
@click.option("--format", type=click.Choice(["table", "json", "csv"]), default="table", 
              help="Output format")
def query(sql: Optional[str], file: Optional[str], project: Optional[str], 
          dataset: Optional[str], limit: int, format: str):
    """Run a BigQuery query and display the results."""
    if not sql and not file:
        sql = sys.stdin.read().strip() if not sys.stdin.isatty() else None
        if not sql:
            console.print("[bold red]Error:[/] No SQL provided. Use a SQL string, --file, or pipe SQL to stdin.")
            sys.exit(1)
    elif file:
        try:
            with open(file, "r") as f:
                sql = f.read().strip()
        except FileNotFoundError:
            console.print(f"[bold red]Error:[/] File {file} not found")
            sys.exit(1)

    result = run_query(sql, project, dataset, limit, format)
    if result.get("error"):
        console.print(f"[bold red]Error:[/] {result['error']}")
        sys.exit(1)
    
    # Output is handled by run_query since it uses rich formatting


@cli.command()
@click.argument("sql", required=False)
@click.option("--file", "-f", help="SQL file path")
@click.option("--project", "-p", help="GCP project ID")
@click.option("--dataset", "-d", help="BigQuery dataset")
@click.option("--format", type=click.Choice(["table", "json"]), default="table",
              help="Output format")
def dryrun(sql: Optional[str], file: Optional[str], project: Optional[str], 
           dataset: Optional[str], format: str):
    """Validate a query with BigQuery's dry run feature."""
    if not sql and not file:
        sql = sys.stdin.read().strip() if not sys.stdin.isatty() else None
        if not sql:
            console.print("[bold red]Error:[/] No SQL provided. Use a SQL string, --file, or pipe SQL to stdin.")
            sys.exit(1)
    elif file:
        try:
            with open(file, "r") as f:
                sql = f.read().strip()
        except FileNotFoundError:
            console.print(f"[bold red]Error:[/] File {file} not found")
            sys.exit(1)

    result = dry_run_query(sql, project, dataset, format)
    if result.get("error"):
        console.print(f"[bold red]Error:[/] {result['error']}")
        sys.exit(1)
    
    # Success output is handled in dry_run_query


@cli.command()
@click.argument("table")
@click.option("--project", "-p", help="GCP project ID")
@click.option("--format", type=click.Choice(["table", "json"]), default="table",
              help="Output format")
@click.option("--detailed/--simple", default=False, help="Include detailed field information")
def schema(table: str, project: Optional[str], format: str, detailed: bool):
    """Get and display the schema of a BigQuery table."""
    result = get_schema(table, project, format, detailed)
    if result.get("error"):
        console.print(f"[bold red]Error:[/] {result['error']}")
        sys.exit(1)
    
    # Output is handled by get_schema


@cli.command()
@click.argument("table1")
@click.argument("table2")
@click.option("--project", "-p", help="GCP project ID")
@click.option("--format", type=click.Choice(["rich", "text", "json"]), default="rich",
              help="Output format")
def diff(table1: str, table2: str, project: Optional[str], format: str):
    """Compare schemas between two BigQuery tables."""
    result = diff_schemas(table1, table2, project, format)
    if result.get("error"):
        console.print(f"[bold red]Error:[/] {result['error']}")
        sys.exit(1)
    
    # Output is handled by diff_schemas


@cli.command()
@click.argument("file", required=False)
@click.option("--fix/--no-fix", default=False, help="Automatically fix linting issues")
@click.option("--output", "-o", help="Output file for fixed SQL")
def lint(file: Optional[str], fix: bool, output: Optional[str]):
    """Lint and optionally format SQL files."""
    if not file:
        sql = sys.stdin.read() if not sys.stdin.isatty() else None
        if not sql:
            console.print("[bold red]Error:[/] No SQL provided. Use a file path or pipe SQL to stdin.")
            sys.exit(1)
    else:
        try:
            with open(file, "r") as f:
                sql = f.read()
        except FileNotFoundError:
            console.print(f"[bold red]Error:[/] File {file} not found")
            sys.exit(1)

    result = lint_sql(sql, fix)
    
    if fix and output:
        with open(output, "w") as f:
            f.write(result["fixed_sql"])
        console.print(f"[bold green]Fixed SQL written to:[/] {output}")
    elif fix:
        if file:
            with open(file, "w") as f:
                f.write(result["fixed_sql"])
            console.print(f"[bold green]Fixed SQL written to:[/] {file}")
        else:
            # Write to stdout
            print(result["fixed_sql"])
    
    # Show linting issues
    if result.get("issues"):
        for issue in result["issues"]:
            console.print(f"[yellow]Line {issue['line']}:[/] {issue['message']}")
        
        if len(result["issues"]) > 0:
            sys.exit(1)


if __name__ == "__main__":
    cli()
