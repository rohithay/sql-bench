"""
Tests for the query functionality.
"""
import unittest
from unittest import mock
import json

from google.cloud import bigquery

from bench.query import run_query, dry_run_query


class MockQueryJob:
    """Mock for a BigQuery QueryJob."""
    
    def __init__(self, rows=None, total_bytes_processed=1024):
        self.rows = rows or []
        self.total_bytes_processed = total_bytes_processed
    
    def result(self):
        """Return the mock result."""
        return self


class TestQuery(unittest.TestCase):
    """Test cases for query functionality."""
    
    @mock.patch('bench.query.get_client')
    def test_dry_run_valid_query(self, mock_get_client):
        """Test dry run with a valid query."""
        # Setup mock
        mock_client = mock.MagicMock()
        mock_query_job = MockQueryJob()
        mock_client.query.return_value = mock_query_job
        mock_get_client.return_value = mock_client
        
        # Run the function
        result = dry_run_query("SELECT * FROM `project.dataset.table`", output_format="json")
        
        # Assertions
        self.assertTrue(result["valid"])
        self.assertEqual(result["bytes_to_be_processed"], 1024)
        
        # Verify mock calls
        mock_client.query.assert_called_once()
        job_config = mock_client.query.call_args[1]["job_config"]
        self.assertTrue(job_config.dry_run)
        self.assertFalse(job_config.use_query_cache)
    
    @mock.patch('bench.query.get_client')
    def test_dry_run_invalid_query(self, mock_get_client):
        """Test dry run with an invalid query."""
        # Setup mock to raise an exception
        mock_client = mock.MagicMock()
        mock_client.query.side_effect = Exception("Invalid syntax")
        mock_get_client.return_value = mock_client
        
        # Run the function
        result = dry_run_query("SELECT * FROM invalid.query", output_format="json")
        
        # Assertions
        self.assertFalse(result["valid"])
        self.assertEqual(result["error"], "Invalid syntax")
    
    @mock.patch('bench.query.get_client')
    def test_run_query(self, mock_get_client):
        """Test running a query."""
        # Create mock schema
        schema = [
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("value", "INTEGER")
        ]
        
        # Create mock rows
        class MockRow(dict):
            pass
        
        row1 = MockRow()
        row1["name"] = "test1"
        row1["value"] = 100
        
        row2 = MockRow()
        row2["name"] = "test2"
        row2["value"] = 200
        
        rows = [row1, row2]
        
        # Setup mock query result
        mock_query_result = mock.MagicMock()
        mock_query_result.schema = schema
        mock_query_result.__iter__.return_value = rows
        
        # Setup mock query job
        mock_query_job = mock.MagicMock()
        mock_query_job.result.return_value = mock_query_result
        mock_query_job.total_bytes_processed = 2048
        
        # Setup mock client
        mock_client = mock.MagicMock()
        mock_client.query.return_value = mock_query_job
        mock_get_client.return_value = mock_client
        
        # Run the function
        result = run_query("SELECT name, value FROM `project.dataset.table`", output_format="json")
        
        # Assertions
        self.assertTrue(result["success"])
        self.assertEqual(result["bytes_processed"], 2048)
        self.assertEqual(result["rows_returned"], 2)
        self.assertEqual(len(result["results"]), 2)
        
        # Verify the client was called with the correct parameters
        mock_client.query.assert_called_once()
    
    @mock.patch('bench.query.get_client')
    def test_run_query_with_error(self, mock_get_client):
        """Test running a query that results in an error."""
        # Setup mock to raise an exception
        mock_client = mock.MagicMock()
        mock_client.query.side_effect = Exception("Query execution failed")
        mock_get_client.return_value = mock_client
        
        # Run the function
        result = run_query("SELECT * FROM non_existent_table")
        
        # Assertions
        self.assertEqual(result["error"], "Query execution failed")


if __name__ == '__main__':
    unittest.main()
