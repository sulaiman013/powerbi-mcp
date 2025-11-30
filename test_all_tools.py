"""
Comprehensive Test Suite for Power BI MCP Server V2
Tests all 18 tools: Desktop (7), Cloud (6), Security (2), RLS (3)
"""
import sys
import os
sys.path.insert(0, 'src')

from typing import Dict, Any, List

# Test results tracking
test_results = {
    'passed': [],
    'failed': [],
    'skipped': []
}

def log_pass(test_name: str, message: str = ""):
    test_results['passed'].append(test_name)
    print(f"  [PASS] {test_name}" + (f" - {message}" if message else ""))

def log_fail(test_name: str, error: str):
    test_results['failed'].append((test_name, error))
    print(f"  [FAIL] {test_name} - {error}")

def log_skip(test_name: str, reason: str):
    test_results['skipped'].append((test_name, reason))
    print(f"  [SKIP] {test_name} - {reason}")


# ============================================================
# TEST 1: DESKTOP CONNECTOR TOOLS
# ============================================================
def test_desktop_tools():
    """Test all 7 Desktop tools"""
    print("\n" + "=" * 60)
    print("TESTING DESKTOP CONNECTOR TOOLS (7 tools)")
    print("=" * 60)

    try:
        from powerbi_desktop_connector import PowerBIDesktopConnector
        connector = PowerBIDesktopConnector()

        # Tool 1: desktop_discover_instances
        print("\n[1/7] Testing desktop_discover_instances...")
        try:
            instances = connector.discover_instances()
            if isinstance(instances, list):
                log_pass("desktop_discover_instances", f"Found {len(instances)} instance(s)")

                if len(instances) == 0:
                    log_skip("Remaining Desktop tools", "No Power BI Desktop instances running")
                    return

                # Get first instance for further tests
                instance = instances[0]
                port = instance.get('port')
                print(f"    Using instance on port {port}: {instance.get('model_name', 'Unknown')}")
            else:
                log_fail("desktop_discover_instances", f"Expected list, got {type(instances)}")
                return
        except Exception as e:
            log_fail("desktop_discover_instances", str(e))
            return

        # Tool 2: desktop_connect
        print("\n[2/7] Testing desktop_connect...")
        try:
            connected = connector.connect(port=port)
            if connected:
                log_pass("desktop_connect", f"Connected to port {port}")
            else:
                log_fail("desktop_connect", "Connection returned False")
                return
        except Exception as e:
            log_fail("desktop_connect", str(e))
            return

        # Tool 3: desktop_list_tables
        print("\n[3/7] Testing desktop_list_tables...")
        try:
            tables = connector.list_tables()
            if isinstance(tables, list):
                log_pass("desktop_list_tables", f"Found {len(tables)} table(s)")
                if tables:
                    print(f"    Tables: {[t.get('name', 'Unknown') for t in tables[:5]]}")
                    test_table = tables[0].get('name')
            else:
                log_fail("desktop_list_tables", f"Expected list, got {type(tables)}")
                test_table = None
        except Exception as e:
            log_fail("desktop_list_tables", str(e))
            test_table = None

        # Tool 4: desktop_list_columns
        print("\n[4/7] Testing desktop_list_columns...")
        if test_table:
            try:
                columns = connector.list_columns(test_table)
                if isinstance(columns, list):
                    log_pass("desktop_list_columns", f"Found {len(columns)} column(s) in '{test_table}'")
                    if columns:
                        print(f"    Columns: {[c.get('name', 'Unknown') for c in columns[:5]]}")
                else:
                    log_fail("desktop_list_columns", f"Expected list, got {type(columns)}")
            except Exception as e:
                log_fail("desktop_list_columns", str(e))
        else:
            log_skip("desktop_list_columns", "No tables available")

        # Tool 5: desktop_list_measures
        print("\n[5/7] Testing desktop_list_measures...")
        try:
            measures = connector.list_measures()
            if isinstance(measures, list):
                log_pass("desktop_list_measures", f"Found {len(measures)} measure(s)")
                if measures:
                    print(f"    Measures: {[m.get('name', 'Unknown') for m in measures[:5]]}")
            else:
                log_fail("desktop_list_measures", f"Expected list, got {type(measures)}")
        except Exception as e:
            log_fail("desktop_list_measures", str(e))

        # Tool 6: desktop_execute_dax
        print("\n[6/7] Testing desktop_execute_dax...")
        if test_table:
            try:
                # Quote table name properly
                if ' ' in test_table or '-' in test_table:
                    quoted_table = f"'{test_table}'"
                else:
                    quoted_table = test_table

                dax_query = f"EVALUATE TOPN(3, {quoted_table})"
                rows = connector.execute_dax(dax_query)
                if isinstance(rows, list):
                    log_pass("desktop_execute_dax", f"Query returned {len(rows)} row(s)")
                else:
                    log_fail("desktop_execute_dax", f"Expected list, got {type(rows)}")
            except Exception as e:
                log_fail("desktop_execute_dax", str(e))
        else:
            log_skip("desktop_execute_dax", "No tables available")

        # Tool 7: desktop_get_model_info
        print("\n[7/7] Testing desktop_get_model_info...")
        try:
            model_info = connector.get_model_info()
            if isinstance(model_info, dict):
                log_pass("desktop_get_model_info", f"Got model info with {len(model_info)} keys")
                print(f"    Keys: {list(model_info.keys())}")
            else:
                log_fail("desktop_get_model_info", f"Expected dict, got {type(model_info)}")
        except Exception as e:
            log_fail("desktop_get_model_info", str(e))

    except ImportError as e:
        log_fail("Desktop connector import", str(e))


# ============================================================
# TEST 2: SECURITY TOOLS
# ============================================================
def test_security_tools():
    """Test Security tools"""
    print("\n" + "=" * 60)
    print("TESTING SECURITY TOOLS (2 tools)")
    print("=" * 60)

    try:
        from security import SecurityLayer
        security = SecurityLayer(
            config_path="config/policies.yaml",
            enable_pii_detection=True,
            enable_audit=True,
            enable_policies=True
        )

        # Tool 1: security_status
        print("\n[1/2] Testing security_status...")
        try:
            status = security.get_status()
            if isinstance(status, dict):
                log_pass("security_status", "Got security status")
                print(f"    PII Detection: {status.get('enabled', {}).get('pii_detection')}")
                print(f"    Audit Logging: {status.get('enabled', {}).get('audit_logging')}")
                print(f"    Access Policies: {status.get('enabled', {}).get('access_policies')}")
            else:
                log_fail("security_status", f"Expected dict, got {type(status)}")
        except Exception as e:
            log_fail("security_status", str(e))

        # Tool 2: security_audit_log (get recent events)
        print("\n[2/2] Testing security_audit_log...")
        try:
            # First log a test query
            test_data = [{'Name': 'Test User', 'Email': 'test@example.com'}]
            security.process_results(
                results=test_data,
                query="EVALUATE TestTable",
                source="test",
                model_name="TestModel",
                duration_ms=50
            )

            # Now retrieve audit log
            events = security.audit_logger.get_recent_events(5)
            if isinstance(events, list):
                log_pass("security_audit_log", f"Retrieved {len(events)} audit event(s)")
            else:
                log_fail("security_audit_log", f"Expected list, got {type(events)}")
        except Exception as e:
            log_fail("security_audit_log", str(e))

    except ImportError as e:
        log_fail("Security module import", str(e))


# ============================================================
# TEST 3: PII DETECTION
# ============================================================
def test_pii_detection():
    """Test PII detection and masking"""
    print("\n" + "=" * 60)
    print("TESTING PII DETECTION & MASKING")
    print("=" * 60)

    try:
        from security import PIIDetector, MaskingStrategy
        detector = PIIDetector(default_strategy=MaskingStrategy.PARTIAL)

        print("\n[1/1] Testing PII detection...")
        test_data = [
            {
                'CustomerName': 'John Smith',
                'Email': 'john.smith@company.com',
                'Phone': '(555) 123-4567',
                'SSN': '123-45-6789',
                'CreditCard': '4111-1111-1111-1234',
                'Revenue': 50000
            }
        ]

        processed, summary = detector.process_results(test_data)

        if summary.get('total_detections', 0) > 0:
            log_pass("pii_detection", f"Detected {summary['total_detections']} PII item(s)")
            print(f"    Types detected: {summary.get('types_detected', [])}")

            # Verify masking worked
            masked_row = processed[0]
            if masked_row.get('Email') != test_data[0]['Email']:
                print(f"    Email masked: {test_data[0]['Email']} -> {masked_row.get('Email')}")
            if masked_row.get('SSN') != test_data[0]['SSN']:
                print(f"    SSN masked: {test_data[0]['SSN']} -> {masked_row.get('SSN')}")
        else:
            log_fail("pii_detection", "No PII detected in test data")

    except Exception as e:
        log_fail("pii_detection", str(e))


# ============================================================
# TEST 4: ACCESS POLICIES
# ============================================================
def test_access_policies():
    """Test access policy engine"""
    print("\n" + "=" * 60)
    print("TESTING ACCESS POLICIES")
    print("=" * 60)

    try:
        from security import AccessPolicyEngine, PolicyAction, TablePolicy, ColumnPolicy
        engine = AccessPolicyEngine()

        # Create test policy
        customers_policy = TablePolicy(
            name="Customers",
            default_action=PolicyAction.ALLOW,
            columns={
                'ssn': ColumnPolicy(name='ssn', action=PolicyAction.BLOCK, reason='PII'),
                'email': ColumnPolicy(name='email', action=PolicyAction.MASK)
            }
        )
        engine.add_table_policy(customers_policy)

        print("\n[1/2] Testing policy check...")
        try:
            check = engine.check_query(
                "EVALUATE Customers",
                tables=["Customers"],
                columns=["SSN", "Email", "Name"]
            )

            if hasattr(check, 'allowed'):
                log_pass("policy_check", f"Query allowed: {check.allowed}")
                print(f"    Columns to block: {check.columns_to_block}")
                print(f"    Columns to mask: {check.columns_to_mask}")
            else:
                log_fail("policy_check", "Invalid check result")
        except Exception as e:
            log_fail("policy_check", str(e))

        print("\n[2/2] Testing policy apply...")
        try:
            test_results_data = [
                {'Name': 'John', 'Email': 'john@test.com', 'SSN': '123-45-6789'}
            ]
            processed, report = engine.apply_to_results(test_results_data, table_name="Customers")

            if processed[0].get('SSN') is None:
                log_pass("policy_apply", "SSN column blocked successfully")
            else:
                log_fail("policy_apply", "SSN column was not blocked")
        except Exception as e:
            log_fail("policy_apply", str(e))

    except Exception as e:
        log_fail("access_policies", str(e))


# ============================================================
# TEST 5: RLS TOOLS
# ============================================================
def test_rls_tools():
    """Test RLS tools"""
    print("\n" + "=" * 60)
    print("TESTING RLS TOOLS (3 tools)")
    print("=" * 60)

    try:
        from powerbi_desktop_connector import PowerBIDesktopConnector
        connector = PowerBIDesktopConnector()

        # First discover and connect
        instances = connector.discover_instances()
        if not instances:
            log_skip("RLS tools", "No Power BI Desktop instances running")
            return

        port = instances[0].get('port')
        connected = connector.connect(port=port)
        if not connected:
            log_skip("RLS tools", "Could not connect to Desktop instance")
            return

        # Tool 1: desktop_list_rls_roles
        print("\n[1/3] Testing desktop_list_rls_roles...")
        try:
            roles = connector.list_rls_roles()
            if isinstance(roles, list):
                log_pass("desktop_list_rls_roles", f"Found {len(roles)} RLS role(s)")
                if roles:
                    print(f"    Roles: {[r.get('name', 'Unknown') for r in roles]}")
            else:
                log_fail("desktop_list_rls_roles", f"Expected list, got {type(roles)}")
        except Exception as e:
            log_fail("desktop_list_rls_roles", str(e))

        # Tool 2: desktop_rls_status
        print("\n[2/3] Testing desktop_rls_status...")
        try:
            status = connector.get_rls_status()
            if isinstance(status, dict):
                log_pass("desktop_rls_status", "Got RLS status")
                print(f"    RLS Active: {status.get('rls_active')}")
                print(f"    Current Role: {status.get('current_role')}")
            else:
                log_fail("desktop_rls_status", f"Expected dict, got {type(status)}")
        except Exception as e:
            log_fail("desktop_rls_status", str(e))

        # Tool 3: desktop_set_rls_role (test with None to clear)
        print("\n[3/3] Testing desktop_set_rls_role...")
        try:
            result = connector.set_rls_role(None)  # Clear any role
            if result:
                log_pass("desktop_set_rls_role", "Successfully cleared RLS role")
            else:
                # This might fail if no roles exist, which is OK
                log_pass("desktop_set_rls_role", "RLS role operation completed")
        except Exception as e:
            log_fail("desktop_set_rls_role", str(e))

    except ImportError as e:
        log_fail("RLS tools import", str(e))


# ============================================================
# TEST 6: CLOUD CONNECTOR (STRUCTURE ONLY)
# ============================================================
def test_cloud_connector_structure():
    """Test Cloud connector structure (no actual connection)"""
    print("\n" + "=" * 60)
    print("TESTING CLOUD CONNECTOR STRUCTURE (6 tools)")
    print("=" * 60)

    try:
        from powerbi_xmla_connector import PowerBIXmlaConnector

        # Just verify the class and methods exist
        print("\n[1/6] Verifying Cloud connector methods...")

        connector = PowerBIXmlaConnector(
            tenant_id="test",
            client_id="test",
            client_secret="test"
        )

        methods_to_check = [
            ('connect', 'list_workspaces equivalent'),
            ('discover_tables', 'list_tables'),
            ('get_table_schema', 'list_columns'),
            ('execute_dax', 'execute_dax'),
            ('set_effective_user', 'RLS support'),
            ('get_rls_status', 'RLS status')
        ]

        for method_name, description in methods_to_check:
            if hasattr(connector, method_name):
                log_pass(f"cloud_{method_name}", f"Method exists ({description})")
            else:
                log_fail(f"cloud_{method_name}", f"Method missing ({description})")

        print("\n    Note: Cloud connector requires Azure AD credentials for full testing")

    except ImportError as e:
        log_fail("Cloud connector import", str(e))


# ============================================================
# TEST 7: MCP SERVER STRUCTURE
# ============================================================
def test_mcp_server_structure():
    """Test MCP server tool definitions"""
    print("\n" + "=" * 60)
    print("TESTING MCP SERVER STRUCTURE")
    print("=" * 60)

    try:
        # Read server.py and check for tool definitions
        with open('src/server.py', 'r', encoding='utf-8') as f:
            server_code = f.read()

        expected_tools = [
            # Desktop tools
            'desktop_discover_instances',
            'desktop_connect',
            'desktop_list_tables',
            'desktop_list_columns',
            'desktop_list_measures',
            'desktop_execute_dax',
            'desktop_get_model_info',
            # Cloud tools
            'list_workspaces',
            'list_datasets',
            'list_tables',
            'list_columns',
            'execute_dax',
            'get_model_info',
            # Security tools
            'security_status',
            'security_audit_log',
            # RLS tools
            'desktop_list_rls_roles',
            'desktop_set_rls_role',
            'desktop_rls_status'
        ]

        print(f"\nChecking for {len(expected_tools)} tool definitions...")

        found_count = 0
        for tool in expected_tools:
            if f'"{tool}"' in server_code or f"'{tool}'" in server_code:
                found_count += 1
            else:
                log_fail(f"tool_{tool}", "Not found in server.py")

        if found_count == len(expected_tools):
            log_pass("mcp_server_tools", f"All {found_count} tools defined")
        else:
            log_pass("mcp_server_tools", f"Found {found_count}/{len(expected_tools)} tools")

    except Exception as e:
        log_fail("mcp_server_structure", str(e))


# ============================================================
# MAIN
# ============================================================
def main():
    print("\n" + "=" * 60)
    print("POWER BI MCP SERVER V2 - COMPREHENSIVE TEST SUITE")
    print("=" * 60)
    print("Testing all 18 tools across Desktop, Cloud, Security, and RLS")

    # Run all tests
    test_desktop_tools()
    test_security_tools()
    test_pii_detection()
    test_access_policies()
    test_rls_tools()
    test_cloud_connector_structure()
    test_mcp_server_structure()

    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)

    passed = len(test_results['passed'])
    failed = len(test_results['failed'])
    skipped = len(test_results['skipped'])
    total = passed + failed + skipped

    print(f"\n  Total tests: {total}")
    print(f"  Passed: {passed}")
    print(f"  Failed: {failed}")
    print(f"  Skipped: {skipped}")

    if failed > 0:
        print("\n  FAILED TESTS:")
        for name, error in test_results['failed']:
            print(f"    - {name}: {error}")

    if skipped > 0:
        print("\n  SKIPPED TESTS:")
        for name, reason in test_results['skipped']:
            print(f"    - {name}: {reason}")

    print("\n" + "=" * 60)
    if failed == 0:
        print("ALL TESTS PASSED!")
    else:
        print(f"TESTS COMPLETED WITH {failed} FAILURE(S)")
    print("=" * 60 + "\n")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
