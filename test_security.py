"""Test security features - PII Detection, Audit Logging, Access Policies"""
import sys
sys.path.insert(0, 'src')

from security import (
    PIIDetector, MaskingStrategy, PIIType,
    AuditLogger,
    AccessPolicyEngine, PolicyAction, TablePolicy, ColumnPolicy,
    SecurityLayer
)

def test_pii_detection():
    """Test PII detection and masking"""
    print("\n" + "=" * 60)
    print("TEST 1: PII DETECTION & MASKING")
    print("=" * 60)

    detector = PIIDetector(default_strategy=MaskingStrategy.PARTIAL)

    # Test data with various PII types
    test_data = [
        {
            'CustomerName': 'John Smith',
            'Email': 'john.smith@company.com',
            'Phone': '(555) 123-4567',
            'SSN': '123-45-6789',
            'Revenue': 50000
        },
        {
            'CustomerName': 'Jane Doe',
            'Email': 'jane.doe@example.org',
            'Phone': '555-987-6543',
            'CreditCard': '4111-1111-1111-1234',
            'Revenue': 75000
        }
    ]

    print("\nOriginal data:")
    for row in test_data:
        print(f"  {row}")

    processed, summary = detector.process_results(test_data)

    print("\nMasked data:")
    for row in processed:
        print(f"  {row}")

    print(f"\nPII Detection Summary:")
    print(f"  Total detections: {summary['total_detections']}")
    print(f"  Types detected: {summary['types_detected']}")

    assert summary['total_detections'] > 0, "Should detect PII"
    print("\n[PASS] PII Detection test PASSED")


def test_audit_logging():
    """Test audit logging"""
    print("\n" + "=" * 60)
    print("TEST 2: AUDIT LOGGING")
    print("=" * 60)

    logger = AuditLogger(log_dir="./logs", log_file="test_audit.log")

    # Log a query
    event = logger.log_query(
        query="EVALUATE Sales",
        source="desktop",
        model_name="TestModel",
        port=12345,
        result_count=100,
        duration_ms=150.5,
        success=True,
        pii_detected=True,
        pii_types=["email", "phone"],
        pii_count=5
    )

    print(f"\nLogged query event:")
    print(f"  Query ID: {event['query_id']}")
    print(f"  Timestamp: {event['timestamp']}")

    # Log a policy violation
    violation = logger.log_policy_violation(
        policy_name="ssn_block",
        violation_type="blocked_column",
        table="Customers",
        column="SSN",
        query="EVALUATE Customers"
    )

    print(f"\nLogged policy violation:")
    print(f"  Policy: {violation['details']['policy']}")

    # Read back events
    events = logger.get_recent_events(10)
    print(f"\nRecent events in log: {len(events)}")

    summary = logger.get_session_summary()
    print(f"Session summary: {summary}")

    print("\n[PASS] Audit Logging test PASSED")


def test_access_policies():
    """Test access policy engine"""
    print("\n" + "=" * 60)
    print("TEST 3: ACCESS POLICIES")
    print("=" * 60)

    engine = AccessPolicyEngine()

    # Create a policy for Customers table
    customers_policy = TablePolicy(
        name="Customers",
        default_action=PolicyAction.ALLOW,
        columns={
            'ssn': ColumnPolicy(name='ssn', action=PolicyAction.BLOCK, reason='PII'),
            'email': ColumnPolicy(name='email', action=PolicyAction.MASK),
            'name': ColumnPolicy(name='name', action=PolicyAction.ALLOW)
        },
        max_rows=100
    )
    engine.add_table_policy(customers_policy)

    print("\nPolicy configuration:")
    print(f"  Table: Customers")
    print(f"  SSN column: BLOCK")
    print(f"  Email column: MASK")
    print(f"  Name column: ALLOW")

    # Test query check
    check = engine.check_query(
        "EVALUATE Customers",
        tables=["Customers"],
        columns=["SSN", "Email", "Name"]
    )

    print(f"\nQuery check result:")
    print(f"  Allowed: {check.allowed}")
    print(f"  Violations: {len(check.violations)}")
    print(f"  Columns to block: {check.columns_to_block}")
    print(f"  Columns to mask: {check.columns_to_mask}")
    print(f"  Max rows: {check.max_rows}")

    # Test applying policy to results
    test_results = [
        {'Name': 'John', 'Email': 'john@test.com', 'SSN': '123-45-6789'},
        {'Name': 'Jane', 'Email': 'jane@test.com', 'SSN': '987-65-4321'}
    ]

    processed, report = engine.apply_to_results(test_results, table_name="Customers")

    print(f"\nApplied policy to results:")
    print(f"  Blocked columns: {report['blocked_columns']}")
    for row in processed:
        print(f"  {row}")

    # SSN should be None (blocked)
    assert processed[0]['SSN'] is None, "SSN should be blocked"
    print("\n[PASS] Access Policies test PASSED")


def test_security_layer():
    """Test integrated security layer"""
    print("\n" + "=" * 60)
    print("TEST 4: INTEGRATED SECURITY LAYER")
    print("=" * 60)

    # Initialize with config
    security = SecurityLayer(
        config_path="config/policies.yaml",
        enable_pii_detection=True,
        enable_audit=True,
        enable_policies=True
    )

    print("\nSecurity layer initialized")
    status = security.get_status()
    print(f"  PII Detection: {status['enabled']['pii_detection']}")
    print(f"  Audit Logging: {status['enabled']['audit_logging']}")
    print(f"  Access Policies: {status['enabled']['access_policies']}")

    # Test processing results
    test_data = [
        {
            'Customer': 'Alice Brown',
            'Contact': 'alice@example.com',
            'Phone': '555-111-2222',
            'Amount': 1500
        }
    ]

    processed, report = security.process_results(
        results=test_data,
        query="EVALUATE Customers",
        source="desktop",
        model_name="TestModel",
        duration_ms=100
    )

    print(f"\nProcessed results through security layer:")
    print(f"  PII detected: {report['pii_detected']}")
    print(f"  PII count: {report['pii_count']}")
    print(f"  PII types: {report['pii_types']}")
    print(f"  Processing time: {report['processing_time_ms']:.2f}ms")

    print(f"\nMasked output:")
    for row in processed:
        print(f"  {row}")

    print("\n[PASS] Security Layer test PASSED")


def main():
    print("\n" + "=" * 60)
    print("POWER BI MCP SECURITY FEATURES TEST SUITE")
    print("=" * 60)

    try:
        test_pii_detection()
        test_audit_logging()
        test_access_policies()
        test_security_layer()

        print("\n" + "=" * 60)
        print("ALL SECURITY TESTS PASSED!")
        print("=" * 60 + "\n")

    except Exception as e:
        print(f"\n[FAIL] Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
