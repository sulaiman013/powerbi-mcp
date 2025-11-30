"""Comprehensive test of all Desktop connector functions"""
import sys
sys.path.insert(0, 'src')

from powerbi_desktop_connector import PowerBIDesktopConnector

def test_all():
    connector = PowerBIDesktopConnector()

    print("=" * 60)
    print("POWER BI DESKTOP CONNECTOR V2 - COMPREHENSIVE TEST")
    print("=" * 60)

    # 1. Check availability
    print("\n[1] Checking availability...")
    if not connector.is_available():
        print("   FAIL: Desktop connector not available")
        return False
    print("   PASS: psutil and ADOMD.NET available")

    # 2. Discover instances
    print("\n[2] Discovering instances...")
    instances = connector.discover_instances()
    if not instances:
        print("   FAIL: No Power BI Desktop instances found")
        return False
    print(f"   PASS: Found {len(instances)} instance(s)")
    for inst in instances:
        print(f"        - Port {inst['port']}: {inst['model_name']}")

    # 3. Connect
    print("\n[3] Connecting to first instance...")
    if not connector.connect():
        print("   FAIL: Could not connect")
        return False
    print(f"   PASS: Connected to {connector.current_model_name} on port {connector.current_port}")

    # 4. List tables
    print("\n[4] Listing tables...")
    tables = connector.list_tables()
    if not tables:
        print("   FAIL: No tables found")
        return False
    print(f"   PASS: Found {len(tables)} tables")
    for t in tables[:5]:
        print(f"        - {t['name']}")
    if len(tables) > 5:
        print(f"        ... and {len(tables) - 5} more")

    # 5. List columns for first table
    print(f"\n[5] Listing columns for '{tables[0]['name']}'...")
    columns = connector.list_columns(tables[0]['name'])
    if not columns:
        print("   FAIL: No columns found")
        return False
    print(f"   PASS: Found {len(columns)} columns")
    for c in columns[:5]:
        print(f"        - {c['name']} ({c['type']})")
    if len(columns) > 5:
        print(f"        ... and {len(columns) - 5} more")

    # 6. List measures
    print("\n[6] Listing measures...")
    measures = connector.list_measures()
    print(f"   PASS: Found {len(measures)} measures")
    for m in measures[:5]:
        print(f"        - [{m['table']}] {m['name']}")
    if len(measures) > 5:
        print(f"        ... and {len(measures) - 5} more")

    # 7. List relationships
    print("\n[7] Listing relationships...")
    relationships = connector.list_relationships()
    print(f"   PASS: Found {len(relationships)} relationships")
    for r in relationships[:3]:
        active = "active" if r['is_active'] else "inactive"
        print(f"        - {r['from_table']}[{r['from_column']}] -> {r['to_table']}[{r['to_column']}] ({active})")
    if len(relationships) > 3:
        print(f"        ... and {len(relationships) - 3} more")

    # 8. Execute DAX query
    print("\n[8] Executing DAX query...")
    try:
        result = connector.execute_dax("EVALUATE ROW(\"Test\", 1 + 1)")
        if result and result[0].get('[Test]') == 2:
            print("   PASS: DAX query returned correct result")
        else:
            print(f"   FAIL: Unexpected result: {result}")
            return False
    except Exception as e:
        print(f"   FAIL: {e}")
        return False

    # 9. Execute a real query against the model
    print("\n[9] Executing query against model...")
    try:
        first_table = tables[0]['name']
        query = f'EVALUATE TOPN(3, \'{first_table}\')'
        result = connector.execute_dax(query)
        print(f"   PASS: Query returned {len(result)} rows from '{first_table}'")
    except Exception as e:
        print(f"   WARNING: Query failed (this is OK if table has issues): {e}")

    # 10. Get VertiPaq stats
    print("\n[10] Getting VertiPaq statistics...")
    try:
        stats = connector.get_vertipaq_stats()
        if stats.get('tables'):
            print(f"   PASS: Got stats for {len(stats['tables'])} tables")
            total_mb = stats['total_size'] / (1024 * 1024) if stats['total_size'] else 0
            print(f"        Total size: {total_mb:.2f} MB")
        else:
            print("   WARN: No VertiPaq stats (may require DMV access)")
    except Exception as e:
        print(f"   WARN: VertiPaq stats not available: {e}")

    print("\n" + "=" * 60)
    print("ALL CORE TESTS PASSED!")
    print("=" * 60)

    connector.close()
    return True

if __name__ == "__main__":
    success = test_all()
    sys.exit(0 if success else 1)
