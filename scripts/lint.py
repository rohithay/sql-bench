"""
SQL linting and formatting functionality.
"""
from typing import Dict, List, Any, Optional
import re
import sqlparse

def lint_sql(
    sql: str,
    fix: bool = False
) -> Dict[str, Any]:
    """
    Lint and optionally format SQL.
    
    Args:
        sql: The SQL to lint
        fix: Whether to fix linting issues automatically
    
    Returns:
        Dict containing linting results and optionally fixed SQL
    """
    # Initialize results
    issues = []
    fixed_sql = sql if fix else None
    
    # Check for common issues
    issues.extend(_check_missing_where(sql))
    issues.extend(_check_select_star(sql))
    issues.extend(_check_table_alias(sql))
    issues.extend(_check_column_quotes(sql))
    
    # Format SQL if requested
    if fix:
        fixed_sql = format_sql(sql)
        
        # Apply fixes for common issues
        if issues:
            # Fix missing aliases
            for issue in issues:
                if issue.get("fix") and issue.get("regex"):
                    fixed_sql = re.sub(
                        issue["regex"],
                        issue["replacement"],
                        fixed_sql
                    )
    
    return {
        "issues": issues,
        "fixed_sql": fixed_sql if fix else None
    }


def format_sql(sql: str) -> str:
    """
    Format SQL using sqlparse.
    
    Args:
        sql: The SQL to format
    
    Returns:
        Formatted SQL
    """
    return sqlparse.format(
        sql,
        keyword_case='upper',
        identifier_case='lower',
        reindent=True,
        indent_width=4,
        strip_comments=False,
        wrap_after=80
    )


def _check_missing_where(sql: str) -> List[Dict[str, Any]]:
    """Check for DELETE or UPDATE without WHERE clause."""
    issues = []
    
    # Parse the SQL
    statements = sqlparse.parse(sql)
    
    for statement in statements:
        statement_type = statement.get_type()
        
        # Check for DELETE or UPDATE without WHERE
        if statement_type in ('DELETE', 'UPDATE'):
            has_where = False
            
            for token in statement.tokens:
                if token.is_keyword and token.value.upper() == 'WHERE':
                    has_where = True
                    break
            
            if not has_where:
                issues.append({
                    "line": statement.get_start_pos()[0],
                    "message": f"{statement_type} statement without WHERE clause (potential table-wide operation)",
                    "severity": "HIGH"
                })
    
    return issues


def _check_select_star(sql: str) -> List[Dict[str, Any]]:
    """Check for SELECT * usage."""
    issues = []
    
    # Simple regex check for SELECT *
    select_star_pattern = r'SELECT\s+\*\s+FROM'
    matches = re.finditer(select_star_pattern, sql, re.IGNORECASE)
    
    for match in matches:
        # Find the line number
        line_number = sql[:match.start()].count('\n') + 1
        
        issues.append({
            "line": line_number,
            "message": "Use of SELECT * (consider specifying columns explicitly)",
            "severity": "MEDIUM"
        })
    
    return issues


def _check_table_alias(sql: str) -> List[Dict[str, Any]]:
    """Check for tables without aliases."""
    issues = []
    
    # This is a simplified check that might need improvement for complex queries
    from_clause_pattern = r'FROM\s+`?([a-zA-Z0-9._]+)`?(?!\s+AS\s+[a-zA-Z0-9_]+)(?!\s+[a-zA-Z0-9_]+)(\s+JOIN|\s+WHERE|\s+GROUP|\s+ORDER|\s+LIMIT|;|$)'
    matches = re.finditer(from_clause_pattern, sql, re.IGNORECASE)
    
    for match in matches:
        table_name = match.group(1)
        line_number = sql[:match.start()].count('\n') + 1
        
        # Generate a suggested alias from the table name
        suggested_alias = table_name.split('.')[-1][0].lower()
        
        issues.append({
            "line": line_number,
            "message": f"Table '{table_name}' has no alias (suggestion: '{suggested_alias}')",
            "severity": "LOW",
            "fix": True,
            "regex": re.escape(match.group(0)),
            "replacement": f"FROM {table_name} AS {suggested_alias}{match.group(2)}"
        })
    
    return issues


def _check_column_quotes(sql: str) -> List[Dict[str, Any]]:
    """Check for inconsistent column quoting."""
    issues = []
    
    # Check for backtick quotes on some columns but not others
    has_backtick_quotes = '`' in sql
    
    if has_backtick_quotes:
        # Look for unquoted column names in SELECT clauses
        select_pattern = r'SELECT\s+(.*?)\s+FROM'
        select_match = re.search(select_pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if select_match:
            columns_text = select_match.group(1)
            columns = [c.strip() for c in columns_text.split(',')]
            
            quoted_columns = 0
            unquoted_columns = 0
            
            for col in columns:
                # Skip functions and expressions
                if '(' in col or '+' in col or '-' in col or '*' in col or '/' in col:
                    continue
                
                if '`' in col:
                    quoted_columns += 1
                else:
                    unquoted_columns += 1
            
            # If there's a mix of quoted and unquoted columns
            if quoted_columns > 0 and unquoted_columns > 0:
                line_number = sql[:select_match.start()].count('\n') + 1
                
                issues.append({
                    "line": line_number,
                    "message": "Inconsistent column quoting (use backticks consistently for all columns)",
                    "severity": "LOW"
                })
    
    return issues
