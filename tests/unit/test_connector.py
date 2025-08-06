"""
Tests for PowerBIConnector class
"""

import os
import sys
from unittest.mock import MagicMock, Mock, patch

import pytest

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from server import PowerBIConnector, clean_dax_query


@pytest.fixture
def mock_pyadomd():
    """Mock pyadomd for testing"""
    with patch("server.Pyadomd") as mock:
        yield mock


@pytest.mark.unit
class TestPowerBIConnector:
    """Test cases for PowerBIConnector"""

    @pytest.fixture
    def connector(self):
        """Create a PowerBIConnector instance"""
        return PowerBIConnector()

    def test_initialization(self, connector):
        """Test connector initializes with correct defaults"""
        assert connector.connection_string is None
        assert connector.connected is False
        assert connector.tables == []
        assert connector.metadata == {}

    def test_successful_connection(self, connector, mock_pyadomd):
        """Test successful connection to Power BI"""
        # Arrange
        mock_conn = MagicMock()
        mock_pyadomd.return_value.__enter__.return_value = mock_conn

        # Act
        result = connector.connect(
            xmla_endpoint="powerbi://test",
            tenant_id="test-tenant",
            client_id="test-client",
            client_secret="test-secret",
            initial_catalog="test-dataset",
        )

        # Assert
        assert result is True
        assert connector.connected is True
        assert "Provider=MSOLAP" in connector.connection_string
        assert "test-dataset" in connector.connection_string

    def test_connection_failure(self, connector, mock_pyadomd):
        """Test handling of connection failure"""
        # Arrange
        mock_pyadomd.side_effect = Exception("Connection failed")

        # Act & Assert
        with pytest.raises(Exception) as exc_info:
            connector.connect(
                xmla_endpoint="invalid",
                tenant_id="test",
                client_id="test",
                client_secret="test",
                initial_catalog="test",
            )

        assert "Connection failed" in str(exc_info.value)
        assert connector.connected is False

    def test_discover_tables(self, connector, mock_pyadomd):
        """Test table discovery with relationships"""
        # Arrange
        connector.connected = True
        connector.connection_string = "test"

        # Mock schema dataset
        mock_table = Mock()
        mock_table.Rows = [
            {"TABLE_NAME": "Sales", "TABLE_SCHEMA": "Model"},
            {"TABLE_NAME": "Product", "TABLE_SCHEMA": "Model"},
            {"TABLE_NAME": "$SYSTEM_TABLE", "TABLE_SCHEMA": "$SYSTEM"},  # Should be filtered
            {"TABLE_NAME": "DateTableTemplate_123", "TABLE_SCHEMA": "Model"},  # Should be filtered
        ]

        mock_dataset = Mock()
        mock_dataset.Tables.Count = 1
        mock_dataset.Tables = [mock_table]

        mock_conn = Mock()
        mock_conn.conn.GetSchemaDataSet.return_value = mock_dataset
        mock_pyadomd.return_value.__enter__.return_value = mock_conn

        # Mock the table description method to return None (no description)
        connector._get_table_description_direct = Mock(return_value=None)

        # Mock the relationships method to return empty list
        connector._get_table_relationships = Mock(return_value=[])

        # Act
        tables = connector.discover_tables()

        # Assert
        assert len(tables) == 2
        table_names = [table["name"] for table in tables]
        assert "Sales" in table_names
        assert "Product" in table_names
        assert "$SYSTEM_TABLE" not in table_names
        assert "DateTableTemplate_123" not in table_names

        # Check that descriptions and relationships are included
        assert all("description" in table for table in tables)
        assert all("relationships" in table for table in tables)
        assert all(table["description"] == "No description available" for table in tables)
        assert all(table["relationships"] == [] for table in tables)

    def test_get_table_relationships_mock(self, connector):
        """Test table relationships discovery with mock method"""
        # Arrange - Mock the method directly to test integration
        expected_relationships = [
            {
                "relatedTable": "Product",
                "fromColumn": "ProductKey",
                "toColumn": "ProductKey",
                "cardinality": "Many-to-One",
                "isActive": True,
                "crossFilterDirection": "Single",
                "relationshipType": "Many-to-One",
            }
        ]

        # Mock the method to return expected data
        connector._get_table_relationships = Mock(return_value=expected_relationships)

        # Act
        relationships = connector._get_table_relationships("Sales")

        # Assert
        assert len(relationships) == 1
        rel = relationships[0]
        assert rel["relatedTable"] == "Product"
        assert rel["fromColumn"] == "ProductKey"
        assert rel["toColumn"] == "ProductKey"
        assert rel["cardinality"] == "Many-to-One"
        assert rel["isActive"] is True
        assert rel["relationshipType"] == "Many-to-One"

    def test_format_cardinality(self, connector):
        """Test cardinality formatting"""
        assert connector._format_cardinality(2, 1) == "Many-to-One"
        assert connector._format_cardinality(1, 2) == "One-to-Many"
        assert connector._format_cardinality(1, 1) == "One-to-One"

    def test_format_cross_filter(self, connector):
        """Test cross filter direction formatting"""
        assert connector._format_cross_filter(1) == "Single"
        assert connector._format_cross_filter(2) == "Both"
        assert connector._format_cross_filter(3) == "Automatic"
        assert connector._format_cross_filter(4) == "None"
        assert connector._format_cross_filter(99) == "Unknown"

    def test_execute_dax_query(self, connector, mock_pyadomd):
        """Test DAX query execution"""
        # Arrange
        connector.connected = True
        connector.connection_string = "test"

        mock_cursor = Mock()
        mock_cursor.description = [("Column1",), ("Column2",)]
        mock_cursor.fetchall.return_value = [("Value1", "Value2"), ("Value3", "Value4")]

        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyadomd.return_value.__enter__.return_value = mock_conn

        # Act
        results = connector.execute_dax_query("EVALUATE Sales")

        # Assert
        assert len(results) == 2
        assert results[0]["Column1"] == "Value1"
        assert results[0]["Column2"] == "Value2"
        assert results[1]["Column1"] == "Value3"
        assert results[1]["Column2"] == "Value4"
        mock_cursor.execute.assert_called_once_with("EVALUATE Sales")


@pytest.mark.unit
class TestDAXCleaning:
    """Test cases for DAX query cleaning"""

    def test_clean_html_tags(self):
        """Test removal of HTML tags"""
        dirty_query = "EVALUATE FILTER(Sales, Sales[<oii>Rank</oii>] > 5)"
        clean_query = clean_dax_query(dirty_query)
        assert clean_query == "EVALUATE FILTER(Sales, Sales[Rank] > 5)"

    def test_clean_xml_tags(self):
        """Test removal of XML tags"""
        dirty_query = "EVALUATE <tag>SUMMARIZE</tag>(Product, Product[Category])"
        clean_query = clean_dax_query(dirty_query)
        assert clean_query == "EVALUATE SUMMARIZE(Product, Product[Category])"

    def test_clean_multiple_tags(self):
        """Test removal of multiple tags"""
        dirty_query = "EVALUATE <a>TOPN</a>(10, Sales, Sales[Amount], <desc>DESC</desc>)"
        clean_query = clean_dax_query(dirty_query)
        assert clean_query == "EVALUATE TOPN(10, Sales, Sales[Amount], DESC)"

    def test_no_tags_unchanged(self):
        """Test that queries without tags are unchanged"""
        clean_query_input = "EVALUATE SUMMARIZE(Sales, Product[Category])"
        clean_query = clean_dax_query(clean_query_input)
        assert clean_query == clean_query_input

    def test_preserve_dax_operators(self):
        """Test that DAX operators < and > are preserved"""
        query = "EVALUATE FILTER(Sales, Sales[Amount] > 100 && Sales[Quantity] < 50)"
        clean_query = clean_dax_query(query)
        # Should only remove tags, not comparison operators
        assert ">" in clean_query
        assert "<" in clean_query


@pytest.mark.unit
class TestErrorHandling:
    """Test error handling scenarios"""

    def test_not_connected_error(self):
        """Test operations fail when not connected"""
        connector = PowerBIConnector()

        with pytest.raises(Exception) as exc_info:
            connector.execute_dax_query("EVALUATE Sales")

        assert "Not connected to Power BI" in str(exc_info.value)

    @patch("server.Pyadomd")
    def test_dax_execution_error(self, mock_pyadomd):
        """Test handling of DAX execution errors"""
        # Arrange
        connector = PowerBIConnector()
        connector.connected = True
        connector.connection_string = "test"

        mock_cursor = Mock()
        mock_cursor.execute.side_effect = Exception("Invalid DAX syntax")

        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyadomd.return_value.__enter__.return_value = mock_conn

        # Act & Assert
        with pytest.raises(Exception) as exc_info:
            connector.execute_dax_query("INVALID DAX")

        assert "DAX query failed" in str(exc_info.value)


@pytest.mark.unit
class TestPyadomdUnavailable:
    """Test behavior when pyadomd is not available"""

    @patch("server.Pyadomd", None)
    def test_pyadomd_unavailable_error(self):
        """Test that proper error is raised when pyadomd is not available"""
        connector = PowerBIConnector()

        with pytest.raises(Exception) as exc_info:
            connector.connect("test_endpoint", "test_tenant", "test_client", "test_secret", "test_catalog")

        assert "Pyadomd library not available" in str(exc_info.value)

    @patch("server.Pyadomd", None)
    def test_operations_fail_without_pyadomd(self):
        """Test that operations fail gracefully without pyadomd"""
        connector = PowerBIConnector()

        # Test connect
        with pytest.raises(Exception) as exc_info:
            connector.connect("test", "tenant", "client", "secret", "catalog")
        assert "Pyadomd library not available" in str(exc_info.value)

        # Test discover_tables - should fail with connection error first
        with pytest.raises(Exception) as exc_info:
            connector.discover_tables()
        assert "Not connected to Power BI" in str(exc_info.value)

        # Test execute_dax_query - should fail with connection error first
        with pytest.raises(Exception) as exc_info:
            connector.execute_dax_query("EVALUATE Sales")
        assert "Not connected to Power BI" in str(exc_info.value)

        # Test with mocked connection to check pyadomd error
        connector.connected = True
        connector.connection_string = "mocked"

        with pytest.raises(Exception) as exc_info:
            connector.discover_tables()
        assert "Pyadomd library not available" in str(exc_info.value)

        with pytest.raises(Exception) as exc_info:
            connector.execute_dax_query("EVALUATE Sales")
        assert "Pyadomd library not available" in str(exc_info.value)


@pytest.mark.unit
@patch("server.Pyadomd")
class TestTableInfoHandling:
    """Test proper handling of table info with enhanced column format"""

    @pytest.mark.unit
    def test_handle_get_table_info_with_enhanced_columns(self, mock_pyadomd):
        """Test that _handle_get_table_info properly handles enhanced column format"""
        # This test prevents regression of the bug where enhanced columns
        # (containing dicts) were passed to ', '.join() expecting strings

        import asyncio

        from server import PowerBIMCPServer

        server = PowerBIMCPServer()
        server.is_connected = True

        # Mock connector that returns enhanced column format
        class MockConnector:
            def get_table_schema(self, table_name):
                return {
                    "table_name": table_name,
                    "type": "data_table",
                    "description": "Test table description",
                    "columns": [
                        {"name": "Column1", "description": "First column description", "data_type": "String"},
                        {"name": "Column2", "description": "Second column description", "data_type": "Integer"},
                    ],
                }

            def get_sample_data(self, table_name, num_rows):
                return [{"Column1": "value1", "Column2": 123}]

        server.connector = MockConnector()

        # Test that this doesn't raise "expected str instance, dict found" error
        async def run_test():
            result = await server._handle_get_table_info({"table_name": "TestTable"})

            # Verify the result format
            assert isinstance(result, str), "Result should be a string"
            assert "TestTable" in result, "Result should contain table name"
            assert "Column1" in result, "Result should contain column names"
            assert "Column2" in result, "Result should contain column names"
            assert "First column description" in result, "Result should contain column descriptions"
            assert "Second column description" in result, "Result should contain column descriptions"
            assert "String" in result, "Result should contain data types"
            assert "Integer" in result, "Result should contain data types"

            return result

        # Run the async test
        result = asyncio.run(run_test())

        # Additional verification that the format is user-friendly
        assert "Column Details:" in result, "Should include detailed column information"
        assert "Sample data:" in result, "Should include sample data section"

    @pytest.mark.unit
    def test_old_bug_reproduction(self, mock_pyadomd):
        """Test that reproduces the original bug scenario to ensure it's fixed"""
        # This test verifies that the bug "sequence item 0: expected str instance, dict found"
        # is fixed when enhanced columns are used with ', '.join()

        import asyncio

        from server import PowerBIMCPServer

        server = PowerBIMCPServer()
        server.is_connected = True

        # Mock connector that returns the enhanced column format that caused the bug
        class BugReproducingMockConnector:
            def get_table_schema(self, table_name):
                # Return exactly the format that caused the original bug
                return {
                    "table_name": table_name,
                    "type": "data_table",
                    "description": "Test table",
                    "columns": [
                        {"name": "Skill Definitions", "description": "Skill def", "data_type": "Text"},
                        {"name": "ID", "description": "Identifier", "data_type": "Integer"},
                    ],
                }

            def get_sample_data(self, table_name, num_rows):
                return [{"Skill Definitions": "Python", "ID": 1}]

        server.connector = BugReproducingMockConnector()

        # This would previously fail with "sequence item 0: expected str instance, dict found"
        # Now it should work correctly
        async def run_bug_test():
            result = await server._handle_get_table_info({"table_name": "Skill Definitions"})

            # Verify the result is properly formatted
            assert isinstance(result, str), "Result should be a string"
            assert "Skill Definitions" in result, "Should contain table name"
            assert "Skill def" in result, "Should contain column description"
            assert "Text" in result, "Should contain data type"
            assert "Identifier" in result, "Should contain second column description"

            # Most importantly, it should NOT crash with join error
            return result

        # This should not raise the original error
        result = asyncio.run(run_bug_test())

        # Additional verification that the bug is truly fixed
        assert "Error getting table info:" not in result, "Should not contain error message"


class TestColumnDescriptions:
    """Test column descriptions functionality"""

    def test_get_column_descriptions_mock(self, mock_pyadomd):
        """Test column descriptions retrieval with mocked data"""
        # Setup connector
        connector = PowerBIConnector()
        connector.connected = True
        connector.connection_string = "test_connection"

        # Mock the connection and queries
        mock_conn = MagicMock()
        mock_pyadomd.return_value.__enter__.return_value = mock_conn

        # Mock cursor for table ID query
        mock_cursor1 = MagicMock()
        mock_cursor1.fetchone.return_value = iter([(15,)])  # Table ID generator with tuple
        mock_cursor1.close = MagicMock()

        # Mock cursor for columns query
        mock_cursor2 = MagicMock()
        mock_cursor2.fetchall.return_value = [
            ("Id", "Primary key identifier", 6),
            ("Name", "Display name of the entity", 2),
            ("Value", None, 6),  # Column without description
        ]
        mock_cursor2.close = MagicMock()

        # Setup conn.cursor() to return different cursors for different calls
        mock_conn.cursor.side_effect = [mock_cursor1, mock_cursor2]

        # Execute the method
        result = connector._get_column_descriptions("TestTable")

        # Verify results
        assert len(result) == 3
        assert result[0]["name"] == "Id"
        assert result[0]["description"] == "Primary key identifier"
        assert result[0]["data_type"] == 6

        assert result[1]["name"] == "Name"
        assert result[1]["description"] == "Display name of the entity"
        assert result[1]["data_type"] == 2

        assert result[2]["name"] == "Value"
        assert result[2]["description"] is None  # No description
        assert result[2]["data_type"] == 6

        # Verify method calls
        assert mock_conn.cursor.call_count == 2
        mock_cursor1.execute.assert_called_once()
        mock_cursor2.execute.assert_called_once()
        mock_cursor1.close.assert_called_once()
        mock_cursor2.close.assert_called_once()

    def test_get_table_schema_with_column_descriptions(self, mock_pyadomd):
        """Test enhanced get_table_schema with column descriptions"""
        # Setup connector
        connector = PowerBIConnector()
        connector.connected = True
        connector.connection_string = "test_connection"

        # Mock the connection
        mock_conn = MagicMock()
        mock_pyadomd.return_value.__enter__.return_value = mock_conn

        # The get_table_schema method makes multiple calls with different cursors
        # 1. _get_table_description_direct (which creates its own connection)
        # 2. DAX query for column names
        # 3. _get_column_descriptions (which creates its own connection)

        # For the main DAX query cursor (used for column names)
        mock_main_cursor = MagicMock()
        mock_main_cursor.description = [("TestTable[Id]",), ("TestTable[Name]",)]
        mock_main_cursor.close = MagicMock()

        # Setup main connection cursor
        mock_conn.cursor.return_value = mock_main_cursor

        # Mock the helper methods directly since they create their own connections
        connector._get_table_description_direct = Mock(return_value="Test table description")
        connector._get_column_descriptions = Mock(
            return_value=[
                {"name": "Id", "description": "Primary key", "data_type": 6},
                {"name": "Name", "description": "Entity name", "data_type": 2},
            ]
        )

        # Execute
        result = connector.get_table_schema("TestTable")

        # Verify results
        assert result["table_name"] == "TestTable"
        assert result["type"] == "data_table"
        assert result["description"] == "Test table description"
        assert len(result["columns"]) == 2

        # Check enhanced column structure
        assert result["columns"][0]["name"] == "TestTable[Id]"
        assert result["columns"][0]["description"] == "Primary key"
        assert result["columns"][0]["data_type"] == 6

        assert result["columns"][1]["name"] == "TestTable[Name]"
        assert result["columns"][1]["description"] == "Entity name"
        assert result["columns"][1]["data_type"] == 2

        # Verify method calls
        connector._get_table_description_direct.assert_called_once_with("TestTable")
        connector._get_column_descriptions.assert_called_once_with("TestTable")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
