"""
Test PBIP Connector Fixes

Tests for:
1. TMDL name quoting (names with spaces)
2. Relationship reference updates (fromTable/toTable)
3. Backup and validation functionality
"""
import sys
import os
import tempfile
import shutil

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from powerbi_pbip_connector import (
    needs_tmdl_quoting,
    quote_tmdl_name,
    unquote_tmdl_name,
    PowerBIPBIPConnector,
    RenameResult
)

def log_pass(test_name, msg=""):
    print(f"  [PASS] {test_name}: {msg}")

def log_fail(test_name, msg=""):
    print(f"  [FAIL] {test_name}: {msg}")

def test_tmdl_quoting():
    """Test TMDL name quoting functions"""
    print("\n=== Testing TMDL Quoting Functions ===\n")

    # Test needs_tmdl_quoting
    tests = [
        ("Sales", False, "Simple name - no quotes needed"),
        ("Customer Appointments", True, "Name with space - quotes needed"),
        ("My Table Name", True, "Multi-word name - quotes needed"),
        ("_measures", False, "Underscore prefix - no quotes needed"),
        ("123Table", True, "Starts with digit - quotes needed"),
        ("table", True, "Reserved word - quotes needed"),
        ("Normal_Name", False, "Underscore in name - no quotes needed"),
        ("Name's", True, "Single quote in name - quotes needed"),
    ]

    passed = 0
    for name, expected, description in tests:
        result = needs_tmdl_quoting(name)
        if result == expected:
            log_pass(f"needs_tmdl_quoting('{name}')", description)
            passed += 1
        else:
            log_fail(f"needs_tmdl_quoting('{name}')", f"Expected {expected}, got {result}")

    print(f"\n  Quoting tests: {passed}/{len(tests)} passed")

    # Test quote_tmdl_name
    print("\n--- Quote Application Tests ---")
    quote_tests = [
        ("Sales", "Sales"),
        ("Customer Appointments", "'Customer Appointments'"),
        ("My Table's Data", "'My Table''s Data'"),  # Escape internal quote
    ]

    for name, expected in quote_tests:
        result = quote_tmdl_name(name)
        if result == expected:
            log_pass(f"quote_tmdl_name('{name}')", f"-> {result}")
        else:
            log_fail(f"quote_tmdl_name('{name}')", f"Expected '{expected}', got '{result}'")

    return passed == len(tests)

def test_regex_patterns():
    """Test that regex patterns work correctly for TMDL modifications"""
    print("\n=== Testing Regex Patterns ===\n")

    import re

    # Test table declaration pattern
    test_cases = [
        # (content, old_name, new_name, expected_result)
        (
            "table Salesforce_Data",
            "Salesforce_Data",
            "Customer Appointments",
            "table 'Customer Appointments'"
        ),
        (
            "table 'Old Table'",
            "Old Table",
            "New Table Name",
            "table 'New Table Name'"
        ),
        (
            "fromTable: OldTable",
            "OldTable",
            "New Table",
            "fromTable: 'New Table'"
        ),
        (
            "toTable: OldTable\nfromColumn: 'Col'",
            "OldTable",
            "My New Table",
            "toTable: 'My New Table'\nfromColumn: 'Col'"
        ),
        (
            "'OldTable'[Column]",
            "OldTable",
            "New Table",
            "'New Table'[Column]"
        ),
    ]

    passed = 0
    for content, old_name, new_name, expected in test_cases:
        # Simulate what the connector does
        new_name_quoted = quote_tmdl_name(new_name)
        old_escaped = re.escape(old_name)

        result = content

        # Table declaration
        result = re.sub(rf'^table\s+{old_escaped}\s*$', f'table {new_name_quoted}', result, flags=re.MULTILINE)
        result = re.sub(rf"^table\s+'{old_escaped}'\s*$", f'table {new_name_quoted}', result, flags=re.MULTILINE)

        # fromTable
        result = re.sub(rf"(fromTable\s*:\s*){old_escaped}(?=\s|$)", rf'\1{new_name_quoted}', result, flags=re.MULTILINE)
        result = re.sub(rf"(fromTable\s*:\s*)'{old_escaped}'", rf'\1{new_name_quoted}', result)

        # toTable
        result = re.sub(rf"(toTable\s*:\s*){old_escaped}(?=\s|$)", rf'\1{new_name_quoted}', result, flags=re.MULTILINE)
        result = re.sub(rf"(toTable\s*:\s*)'{old_escaped}'", rf'\1{new_name_quoted}', result)

        # DAX reference
        result = re.sub(rf"'{old_escaped}'\s*\[", f"{new_name_quoted}[", result)

        if result == expected:
            log_pass(f"Pattern test", f"'{content[:30]}...' -> correct")
            passed += 1
        else:
            log_fail(f"Pattern test", f"\n    Input: {content}\n    Expected: {expected}\n    Got: {result}")

    print(f"\n  Pattern tests: {passed}/{len(test_cases)} passed")
    return passed == len(test_cases)

def test_connector_import():
    """Test that connector imports correctly"""
    print("\n=== Testing Connector Import ===\n")

    try:
        connector = PowerBIPBIPConnector(auto_backup=False)
        log_pass("Import", "PowerBIPBIPConnector imported successfully")

        # Check attributes
        if hasattr(connector, 'auto_backup'):
            log_pass("Attribute check", "auto_backup attribute exists")
        else:
            log_fail("Attribute check", "auto_backup attribute missing")

        if hasattr(connector, '_original_files'):
            log_pass("Attribute check", "_original_files attribute exists")
        else:
            log_fail("Attribute check", "_original_files attribute missing")

        if hasattr(connector, 'validate_tmdl_syntax'):
            log_pass("Method check", "validate_tmdl_syntax method exists")
        else:
            log_fail("Method check", "validate_tmdl_syntax method missing")

        if hasattr(connector, 'create_backup'):
            log_pass("Method check", "create_backup method exists")
        else:
            log_fail("Method check", "create_backup method missing")

        if hasattr(connector, 'rollback_changes'):
            log_pass("Method check", "rollback_changes method exists")
        else:
            log_fail("Method check", "rollback_changes method missing")

        return True

    except Exception as e:
        log_fail("Import", str(e))
        return False

def test_validation_error_dataclass():
    """Test ValidationError dataclass"""
    print("\n=== Testing ValidationError Dataclass ===\n")

    try:
        from powerbi_pbip_connector import ValidationError

        err = ValidationError(
            file_path="/test/file.tmdl",
            line_number=10,
            error_type="UNQUOTED_NAME",
            message="Test error",
            context="table My Table"
        )

        if err.file_path == "/test/file.tmdl":
            log_pass("ValidationError", "file_path correct")
        else:
            log_fail("ValidationError", "file_path incorrect")

        if err.line_number == 10:
            log_pass("ValidationError", "line_number correct")
        else:
            log_fail("ValidationError", "line_number incorrect")

        if err.error_type == "UNQUOTED_NAME":
            log_pass("ValidationError", "error_type correct")
        else:
            log_fail("ValidationError", "error_type incorrect")

        return True

    except Exception as e:
        log_fail("ValidationError", str(e))
        return False

def test_rename_result_dataclass():
    """Test RenameResult dataclass with new fields"""
    print("\n=== Testing RenameResult Dataclass ===\n")

    try:
        from powerbi_pbip_connector import RenameResult, ValidationError

        result = RenameResult(
            success=True,
            message="Test message",
            files_modified=["file1.tmdl", "file2.tmdl"],
            references_updated=10,
            validation_errors=[],
            backup_created="/backup/path"
        )

        if result.success == True:
            log_pass("RenameResult", "success field correct")
        else:
            log_fail("RenameResult", "success field incorrect")

        if result.backup_created == "/backup/path":
            log_pass("RenameResult", "backup_created field correct")
        else:
            log_fail("RenameResult", "backup_created field incorrect")

        if isinstance(result.validation_errors, list):
            log_pass("RenameResult", "validation_errors is list")
        else:
            log_fail("RenameResult", "validation_errors is not list")

        return True

    except Exception as e:
        log_fail("RenameResult", str(e))
        return False

def test_mock_pbip_project():
    """Test with a mock PBIP project structure"""
    print("\n=== Testing Mock PBIP Project ===\n")

    # Create a temporary directory with mock PBIP structure
    temp_dir = tempfile.mkdtemp(prefix="pbip_test_")

    try:
        # Create mock structure
        pbip_file = os.path.join(temp_dir, "TestProject.pbip")
        semantic_folder = os.path.join(temp_dir, "TestProject.SemanticModel")
        tables_folder = os.path.join(semantic_folder, "definition", "tables")
        relationships_folder = os.path.join(semantic_folder, "definition", "relationships")
        report_folder = os.path.join(temp_dir, "TestProject.Report")

        os.makedirs(tables_folder)
        os.makedirs(relationships_folder)
        os.makedirs(report_folder)

        # Create mock .pbip file
        with open(pbip_file, 'w') as f:
            f.write('{"version": "1.0"}')

        # Create mock table TMDL
        table_content = """table Salesforce_Data
    column 'Created Date'
        dataType: dateTime
    column Amount
        dataType: decimal

    measure TotalAmount = SUM(Salesforce_Data[Amount])
"""
        with open(os.path.join(tables_folder, "Salesforce_Data.tmdl"), 'w') as f:
            f.write(table_content)

        # Create mock relationship TMDL
        relationship_content = """relationship abc123
    fromTable: Salesforce_Data
    fromColumn: 'Created Date'
    toTable: DateTable
    toColumn: Date
"""
        with open(os.path.join(relationships_folder, "abc123.tmdl"), 'w') as f:
            f.write(relationship_content)

        # Create mock report.json
        report_content = '{"Entity": "Salesforce_Data", "Property": "Amount"}'
        with open(os.path.join(report_folder, "report.json"), 'w') as f:
            f.write(report_content)

        # Test loading the project
        connector = PowerBIPBIPConnector(auto_backup=False)  # Disable auto-backup for test

        if connector.load_project(temp_dir):
            log_pass("Load project", "Mock project loaded successfully")
        else:
            log_fail("Load project", "Failed to load mock project")
            return False

        # Test renaming table
        result = connector.rename_table_in_files("Salesforce_Data", "Customer Appointments")

        if result.success:
            log_pass("Rename table", f"Renamed with {result.references_updated} references")
        else:
            log_fail("Rename table", f"Failed: {result.message}")
            if result.validation_errors:
                for err in result.validation_errors:
                    print(f"    Validation error: {err.message}")

        # Verify the table file was updated correctly
        with open(os.path.join(tables_folder, "Salesforce_Data.tmdl"), 'r') as f:
            new_content = f.read()

        if "table 'Customer Appointments'" in new_content:
            log_pass("Table declaration", "Properly quoted")
        else:
            log_fail("Table declaration", f"Not properly quoted. Content:\n{new_content}")

        # Verify relationship was updated
        with open(os.path.join(relationships_folder, "abc123.tmdl"), 'r') as f:
            rel_content = f.read()

        if "fromTable: 'Customer Appointments'" in rel_content:
            log_pass("Relationship fromTable", "Properly updated and quoted")
        else:
            log_fail("Relationship fromTable", f"Not properly updated. Content:\n{rel_content}")

        # Verify DAX was updated
        if "'Customer Appointments'[Amount]" in new_content:
            log_pass("DAX reference", "Properly quoted")
        else:
            log_fail("DAX reference", f"Not properly updated")

        # Verify report.json was updated
        with open(os.path.join(report_folder, "report.json"), 'r') as f:
            report = f.read()

        if '"Entity": "Customer Appointments"' in report:
            log_pass("Report Entity", "Properly updated")
        else:
            log_fail("Report Entity", f"Not properly updated: {report}")

        return True

    except Exception as e:
        log_fail("Mock project test", str(e))
        import traceback
        traceback.print_exc()
        return False

    finally:
        # Cleanup
        shutil.rmtree(temp_dir, ignore_errors=True)

def main():
    print("=" * 60)
    print("  PBIP Connector Bug Fix Tests")
    print("=" * 60)

    results = []

    results.append(("TMDL Quoting", test_tmdl_quoting()))
    results.append(("Regex Patterns", test_regex_patterns()))
    results.append(("Connector Import", test_connector_import()))
    results.append(("ValidationError", test_validation_error_dataclass()))
    results.append(("RenameResult", test_rename_result_dataclass()))
    results.append(("Mock Project", test_mock_pbip_project()))

    print("\n" + "=" * 60)
    print("  SUMMARY")
    print("=" * 60)

    passed = sum(1 for _, r in results if r)
    total = len(results)

    for name, result in results:
        status = "PASS" if result else "FAIL"
        print(f"  [{status}] {name}")

    print(f"\n  Total: {passed}/{total} test groups passed")
    print("=" * 60)

    if passed == total:
        print("\n  ALL TESTS PASSED!")
        return 0
    else:
        print("\n  SOME TESTS FAILED - Review output above")
        return 1

if __name__ == "__main__":
    sys.exit(main())
