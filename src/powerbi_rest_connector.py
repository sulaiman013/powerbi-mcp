"""
Simplified Power BI Connector using REST API
Pure Python - No ADOMD.NET, No pythonnet, Works everywhere!
"""
import logging
import time
from typing import Any, Dict, List, Optional
import requests
import msal

logger = logging.getLogger(__name__)


class PowerBIRestConnector:
    """Simple Power BI connector using REST API only"""

    BASE_URL = "https://api.powerbi.com/v1.0/myorg"
    FABRIC_API_URL = "https://api.fabric.microsoft.com/v1"
    AUTHORITY = "https://login.microsoftonline.com/{tenant_id}"
    SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]

    def __init__(self, tenant_id: str, client_id: str, client_secret: str):
        """Initialize connector with Azure AD credentials"""
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.workspace_id = None
        self.workspace_name = None
        self.dataset_id = None
        self.dataset_name = None

    def authenticate(self) -> bool:
        """Authenticate using Service Principal and get access token"""
        try:
            authority_url = self.AUTHORITY.format(tenant_id=self.tenant_id)
            app = msal.ConfidentialClientApplication(
                self.client_id,
                authority=authority_url,
                client_credential=self.client_secret,
            )

            result = app.acquire_token_for_client(scopes=self.SCOPE)

            if "access_token" in result:
                self.access_token = result["access_token"]
                logger.info("Successfully authenticated to Power BI")
                return True
            else:
                error = result.get("error_description", "Unknown error")
                logger.error(f"Authentication failed: {error}")
                return False

        except Exception as e:
            logger.error(f"Authentication error: {str(e)}")
            return False

    def connect(self, workspace_id: str, dataset_id: str) -> bool:
        """Connect to a specific workspace and dataset"""
        try:
            if not self.access_token:
                if not self.authenticate():
                    return False

            self.workspace_id = workspace_id
            self.dataset_id = dataset_id

            # Get workspace name
            workspaces = self.list_workspaces()
            for ws in workspaces:
                if ws['id'] == workspace_id:
                    self.workspace_name = ws['name']
                    break

            # Verify dataset exists and is accessible
            dataset_info = self.get_dataset_info()
            if dataset_info:
                self.dataset_name = dataset_info.get('name', dataset_id)
                logger.info(f"Connected to dataset: {self.dataset_name}")
                logger.info(f"Workspace: {self.workspace_name or workspace_id}")
                return True
            else:
                logger.error("Failed to access dataset")
                return False

        except Exception as e:
            logger.error(f"Connection error: {str(e)}")
            return False

    def _get_headers(self) -> Dict[str, str]:
        """Get HTTP headers with authorization"""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    def get_dataset_info(self) -> Optional[Dict[str, Any]]:
        """Get information about the dataset"""
        try:
            # Use workspace-scoped endpoint for Service Principal access
            if self.workspace_id:
                url = f"{self.BASE_URL}/groups/{self.workspace_id}/datasets/{self.dataset_id}"
            else:
                url = f"{self.BASE_URL}/datasets/{self.dataset_id}"

            response = requests.get(url, headers=self._get_headers(), timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get dataset info: {str(e)}")
            return None

    def get_model_definition_from_fabric(self) -> List[Dict[str, Any]]:
        """
        Get model definition using Fabric API's getDefinition endpoint
        This returns the complete model schema in TMDL format with all tables and columns

        Returns:
            List of tables with their columns
        """
        try:
            if not self.workspace_id or not self.dataset_id:
                logger.error("Workspace ID and Dataset ID required for Fabric API")
                return []

            logger.info("Using Fabric API to get model definition...")

            # Fabric API endpoint for getting definition
            url = f"{self.FABRIC_API_URL}/workspaces/{self.workspace_id}/items/{self.dataset_id}/getDefinition"

            response = requests.post(url, headers=self._get_headers(), json={}, timeout=60)

            logger.info(f"Fabric API response status: {response.status_code}")

            if response.status_code == 401 or response.status_code == 403:
                logger.warning(f"Fabric API access denied: {response.text}")
                return []

            # Handle Long Running Operation (LRO) - 202 Accepted
            if response.status_code == 202:
                logger.info("Fabric API returned 202 - waiting for long running operation...")

                # Get the location header for polling
                location_url = response.headers.get('Location')
                if not location_url:
                    logger.error("No Location header in 202 response")
                    return []

                logger.info(f"Polling URL: {location_url}")

                # Poll until operation completes
                max_attempts = 30
                attempt = 0
                while attempt < max_attempts:
                    time.sleep(2)  # Wait 2 seconds between polls
                    poll_response = requests.get(location_url, headers=self._get_headers(), timeout=30)

                    logger.info(f"Poll attempt {attempt + 1}: Status {poll_response.status_code}")

                    if poll_response.status_code == 200:
                        # Operation completed - check status
                        lro_status = poll_response.json()
                        status = lro_status.get('status')

                        logger.info(f"LRO status keys: {lro_status.keys()}")

                        if status == 'Succeeded':
                            logger.info("LRO completed successfully!")

                            # Check if definition is in the LRO response itself
                            if 'definition' in lro_status:
                                result = lro_status
                                logger.info("Definition found in LRO response")
                                break

                            # Try getting from result/output field
                            if 'result' in lro_status or 'output' in lro_status:
                                result = lro_status.get('result') or lro_status.get('output')
                                logger.info("Definition found in result/output field")
                                break

                            #Otherwise just use the whole response
                            logger.warning("No definition field found in LRO response - using full response")
                            result = lro_status
                            break

                        elif status == 'Failed':
                            error_info = lro_status.get('error', {})
                            logger.error(f"LRO failed: {error_info}")
                            return []
                        else:
                            logger.info(f"LRO status: {status}")
                            attempt += 1
                            continue
                    elif poll_response.status_code == 202:
                        # Still in progress
                        attempt += 1
                        continue
                    else:
                        logger.error(f"LRO failed: {poll_response.status_code} - {poll_response.text}")
                        return []

                if attempt >= max_attempts:
                    logger.error("LRO timeout - operation did not complete in time")
                    return []
            elif response.status_code == 200:
                # Immediate response
                result = response.json()
            else:
                logger.error(f"Fabric API failed: {response.status_code} - {response.text}")
                return []

            logger.info(f"Fabric API result keys: {result.keys() if result else 'None'}")

            # Check if this is an operation status response
            if 'status' in result:
                operation_status = result.get('status')
                logger.info(f"Operation status: {operation_status}")

                if operation_status == 'Failed':
                    error_info = result.get('error', {})
                    logger.error(f"Operation failed: {error_info}")
                    return []

                # If succeeded, check if there's result data in the response
                logger.info(f"Full result structure: {result}")

            # Parse the definition parts
            tables = []
            definition_parts = result.get("definition", {}).get("parts", [])

            for part in definition_parts:
                # Each part has a path and payload (base64 encoded)
                path = part.get("path", "")
                payload = part.get("payload", "")

                # Decode the base64 payload
                if payload:
                    import base64
                    decoded = base64.b64decode(payload).decode('utf-8')

                    # Parse tables from TMDL format
                    if "table " in decoded.lower():
                        # Extract table information from TMDL
                        tables_from_tmdl = self._parse_tmdl_for_tables(decoded)
                        tables.extend(tables_from_tmdl)

            logger.info(f"Found {len(tables)} tables from Fabric API")
            return tables

        except Exception as e:
            logger.error(f"Fabric API error: {str(e)}")
            import traceback
            logger.debug(traceback.format_exc())
            return []

    def _parse_tmdl_for_tables(self, tmdl_content: str) -> List[Dict[str, Any]]:
        """Parse TMDL content to extract table and column information"""
        tables = []

        try:
            lines = tmdl_content.split('\n')
            current_table = None
            current_columns = []

            for line in lines:
                stripped = line.strip()

                # Detect table definition
                if stripped.startswith('table '):
                    # Save previous table if exists
                    if current_table:
                        tables.append({
                            "name": current_table,
                            "columns": current_columns,
                            "description": "From model definition"
                        })

                    # Extract table name (remove quotes if present)
                    table_name = stripped.replace('table ', '').replace("'", "").strip()
                    current_table = table_name
                    current_columns = []

                # Detect column definition
                elif stripped.startswith('column ') and current_table:
                    # Extract column name and type
                    col_def = stripped.replace('column ', '').strip()

                    # Column format: column 'ColumnName' : DataType
                    if ':' in col_def:
                        col_name = col_def.split(':')[0].replace("'", "").strip()
                        col_type = col_def.split(':')[1].strip() if len(col_def.split(':')) > 1 else "Unknown"
                    else:
                        col_name = col_def.replace("'", "").strip()
                        col_type = "Unknown"

                    current_columns.append({
                        "name": col_name,
                        "type": col_type,
                        "description": ""
                    })

            # Don't forget the last table
            if current_table:
                tables.append({
                    "name": current_table,
                    "columns": current_columns,
                    "description": "From model definition"
                })

        except Exception as e:
            logger.error(f"Error parsing TMDL: {str(e)}")

        return tables

    def list_workspaces(self) -> List[Dict[str, Any]]:
        """
        List all workspaces (groups) accessible by the Service Principal
        https://learn.microsoft.com/en-us/rest/api/power-bi/groups/get-groups
        """
        try:
            if not self.access_token:
                if not self.authenticate():
                    return []

            url = f"{self.BASE_URL}/groups"
            response = requests.get(url, headers=self._get_headers(), timeout=30)
            response.raise_for_status()

            workspaces = response.json().get("value", [])
            logger.info(f"Found {len(workspaces)} workspace(s)")

            return [
                {
                    "id": ws["id"],
                    "name": ws["name"],
                    "type": ws.get("type", "Workspace"),
                    "state": ws.get("state", "Active"),
                    "isReadOnly": ws.get("isReadOnly", False),
                }
                for ws in workspaces
            ]

        except Exception as e:
            logger.error(f"Failed to list workspaces: {str(e)}")
            return []

    def list_datasets(self, workspace_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List all datasets in a workspace or all accessible datasets
        https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-datasets
        """
        try:
            if not self.access_token:
                if not self.authenticate():
                    return []

            if workspace_id:
                # List datasets in specific workspace
                url = f"{self.BASE_URL}/groups/{workspace_id}/datasets"
            else:
                # List all datasets in "My Workspace"
                url = f"{self.BASE_URL}/datasets"

            response = requests.get(url, headers=self._get_headers(), timeout=30)
            response.raise_for_status()

            datasets = response.json().get("value", [])
            logger.info(f"Found {len(datasets)} dataset(s)")

            return [
                {
                    "id": ds["id"],
                    "name": ds["name"],
                    "configuredBy": ds.get("configuredBy", "Unknown"),
                    "isRefreshable": ds.get("isRefreshable", False),
                    "webUrl": ds.get("webUrl", ""),
                }
                for ds in datasets
            ]

        except Exception as e:
            logger.error(f"Failed to list datasets: {str(e)}")
            return []

    def get_tables_via_scanner_api(self) -> List[Dict[str, Any]]:
        """
        Get tables and columns using Power BI Scanner API (Admin API)
        https://learn.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-get-scan-result

        Returns:
            List of tables with their columns
        """
        try:
            if not self.workspace_id:
                logger.error("Workspace ID required for Scanner API")
                return []

            logger.info("Using Scanner API to get dataset schema...")

            # Step 1: Initiate scan
            scan_url = f"{self.BASE_URL}/admin/workspaces/getInfo"
            payload = {
                "workspaces": [self.workspace_id],
                "datasetSchema": True,
                "datasetExpressions": False
            }

            response = requests.post(scan_url, headers=self._get_headers(), json=payload, timeout=30)

            if response.status_code == 401:
                logger.warning("Scanner API requires admin permissions or Tenant.Read.All scope")
                return []

            response.raise_for_status()
            scan_id = response.json().get("id")

            if not scan_id:
                logger.error("No scan ID returned from Scanner API")
                return []

            logger.info(f"Scan initiated with ID: {scan_id}")

            # Step 2: Wait for scan to complete
            status_url = f"{self.BASE_URL}/admin/workspaces/scanStatus/{scan_id}"
            max_attempts = 20
            attempt = 0

            while attempt < max_attempts:
                time.sleep(2)  # Wait 2 seconds between checks
                status_response = requests.get(status_url, headers=self._get_headers(), timeout=30)
                status_response.raise_for_status()

                status = status_response.json().get("status")
                logger.info(f"Scan status: {status}")

                if status == "Succeeded":
                    break
                elif status == "Failed":
                    logger.error("Scanner API scan failed")
                    return []

                attempt += 1

            if attempt >= max_attempts:
                logger.error("Scanner API scan timeout")
                return []

            # Step 3: Get scan results
            result_url = f"{self.BASE_URL}/admin/workspaces/scanResult/{scan_id}"
            result_response = requests.get(result_url, headers=self._get_headers(), timeout=30)
            result_response.raise_for_status()

            scan_result = result_response.json()

            # Parse results
            tables = []
            workspaces = scan_result.get("workspaces", [])

            for workspace in workspaces:
                for dataset in workspace.get("datasets", []):
                    if dataset.get("id") == self.dataset_id:
                        dataset_tables = dataset.get("tables", [])

                        for table in dataset_tables:
                            if not table.get("isHidden", False):
                                table_name = table.get("name", "")
                                if not table_name.startswith("$") and not table_name.startswith("DateTableTemplate"):
                                    # Get columns
                                    columns = []
                                    for col in table.get("columns", []):
                                        if not col.get("isHidden", False):
                                            columns.append({
                                                "name": col.get("name", ""),
                                                "type": col.get("dataType", "Unknown"),
                                                "description": col.get("description", "")
                                            })

                                    tables.append({
                                        "name": table_name,
                                        "description": table.get("description", ""),
                                        "columns": columns
                                    })

            logger.info(f"Scanner API found {len(tables)} tables")
            return tables

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401 or e.response.status_code == 403:
                logger.warning(f"Scanner API access denied: {e.response.status_code}")
                logger.info("Service Principal may need Tenant.Read.All or admin permissions")
            else:
                logger.error(f"Scanner API error: {e.response.status_code} - {e.response.text}")
            return []
        except Exception as e:
            logger.error(f"Scanner API exception: {str(e)}")
            return []

    def execute_dax(self, dax_query: str) -> Dict[str, Any]:
        """
        Execute a DAX query using Power BI REST API
        https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/execute-queries
        """
        try:
            # Use workspace-scoped endpoint for Service Principal access
            if self.workspace_id:
                url = f"{self.BASE_URL}/groups/{self.workspace_id}/datasets/{self.dataset_id}/executeQueries"
            else:
                url = f"{self.BASE_URL}/datasets/{self.dataset_id}/executeQueries"

            payload = {
                "queries": [
                    {
                        "query": dax_query
                    }
                ],
                "serializerSettings": {
                    "includeNulls": True
                }
            }

            logger.info(f"Executing DAX query: {dax_query[:100]}...")
            response = requests.post(
                url,
                headers=self._get_headers(),
                json=payload,
                timeout=60
            )
            response.raise_for_status()

            result = response.json()
            logger.info(f"Query executed successfully")
            return result

        except requests.exceptions.HTTPError as e:
            logger.error(f"DAX query failed: {e.response.status_code} - {e.response.text}")
            raise Exception(f"DAX query failed: {e.response.text}")
        except Exception as e:
            logger.error(f"DAX query error: {str(e)}")
            raise

    def get_tables(self) -> List[Dict[str, Any]]:
        """
        Get list of tables using multiple discovery methods
        Tries Fabric API first (most reliable), then falls back to other methods
        """
        # Method 1: Try Fabric API (most reliable, works with Pro workspaces)
        logger.info("Attempting Fabric API for table discovery...")
        fabric_tables = self.get_model_definition_from_fabric()
        if fabric_tables:
            return fabric_tables

        # Method 2: Try Scanner API (requires admin permissions)
        logger.info("Attempting Scanner API for table discovery...")
        scanner_tables = self.get_tables_via_scanner_api()
        if scanner_tables:
            return scanner_tables

        # Method 2: Try INFO.TABLES() (works in newer datasets)
        try:
            logger.info("Trying INFO.TABLES() to discover tables...")
            dax_query = """
            EVALUATE
            SELECTCOLUMNS(
                INFO.TABLES(),
                "TableName", [Name],
                "Description", [Description],
                "IsHidden", [IsHidden]
            )
            """

            result = self.execute_dax(dax_query)

            # Parse results
            tables = []
            if "results" in result and len(result["results"]) > 0:
                rows = result["results"][0].get("tables", [{}])[0].get("rows", [])
                for row in rows:
                    # Filter out hidden tables and system tables
                    if not row.get("IsHidden", False):
                        table_name = row.get("TableName", "")
                        if not table_name.startswith("$") and not table_name.startswith("DateTableTemplate"):
                            tables.append({
                                "name": table_name,
                                "description": row.get("Description") or "No description available"
                            })

            logger.info(f"Found {len(tables)} tables using INFO.TABLES()")
            return tables

        except Exception as e:
            logger.warning(f"INFO.TABLES() failed: {str(e)}, trying DMV query...")

        # Method 2: Try DMV query (works in most datasets)
        try:
            logger.info("Trying DMV query to discover tables...")
            dax_query = """
            EVALUATE
            SELECTCOLUMNS(
                $SYSTEM.TMSCHEMA_TABLES,
                "TableName", [Name],
                "IsHidden", [IsHidden]
            )
            """

            result = self.execute_dax(dax_query)

            tables = []
            if "results" in result and len(result["results"]) > 0:
                rows = result["results"][0].get("tables", [{}])[0].get("rows", [])
                for row in rows:
                    if not row.get("IsHidden", False):
                        table_name = row.get("TableName", "")
                        if not table_name.startswith("$") and not table_name.startswith("DateTableTemplate"):
                            tables.append({
                                "name": table_name,
                                "description": "No description available"
                            })

            logger.info(f"Found {len(tables)} tables using DMV query")
            return tables

        except Exception as e:
            logger.warning(f"DMV query failed: {str(e)}, trying sampling method...")

        # Method 3: Try discovering tables by sampling common names
        logger.info("Trying table discovery by sampling...")
        sampled_tables = self.discover_tables_by_sampling()
        if sampled_tables:
            return sampled_tables

        # Method 4: Try XMLA endpoint
        logger.info("Trying XMLA endpoint for table discovery...")
        xmla_tables = self.get_tables_via_xmla()
        if xmla_tables:
            return xmla_tables

        # Method 5: Show limitations and return empty
        self._show_discovery_limitations()
        return []

    def _show_discovery_limitations(self):
        """Log information about discovery limitations"""
        logger.warning("Automatic table discovery unavailable")
        logger.info("Table discovery limitations:")
        logger.info("1. Scanner API requires admin permissions")
        logger.info("2. INFO.TABLES() requires newer dataset compatibility")
        logger.info("3. DMV queries require XMLA protocol")
        logger.info("4. XMLA endpoint requires XMLA read-only enabled on workspace")
        logger.info("5. Fallback: Use sampling with known table names")

    def get_tables_via_xmla(self) -> List[Dict[str, Any]]:
        """
        Get tables using XMLA endpoint
        Uses workspace name for better user experience
        Format: powerbi://api.powerbi.com/v1.0/myorg/{WorkspaceName}
        """
        try:
            if not self.workspace_name and not self.workspace_id:
                logger.error("Workspace name or ID required for XMLA")
                return []

            if not self.dataset_name and not self.dataset_id:
                logger.error("Dataset name or ID required for XMLA")
                return []

            logger.info("Using XMLA endpoint to discover tables...")

            # For XMLA over HTTP, we need to use a different endpoint structure
            # The powerbi:// protocol is for ADOMD.NET, but for HTTP/SOAP we use Analysis Services endpoint
            # Try using workspace ID as the data source

            # XMLA endpoint URL using Analysis Services format
            # Format: https://<region>.asazure.windows.net/powerbi/api/v1.0/myorg/<workspace>
            xmla_endpoint = f"https://api.powerbi.com/v1.0/myorg/{self.workspace_id}"
            dataset_identifier = self.dataset_name or self.dataset_id

            logger.info(f"XMLA Endpoint: {xmla_endpoint}")
            logger.info(f"Dataset: {dataset_identifier}")

            # XMLA DISCOVER request to get TMSCHEMA_TABLES
            # This uses SOAP/XML to query the Analysis Services instance
            xmla_request = f"""<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema">
  <soap:Body>
    <Discover xmlns="urn:schemas-microsoft-com:xml-analysis">
      <RequestType>TMSCHEMA_TABLES</RequestType>
      <Restrictions>
        <RestrictionList>
          <CATALOG_NAME>{dataset_identifier}</CATALOG_NAME>
        </RestrictionList>
      </Restrictions>
      <Properties>
        <PropertyList>
          <Catalog>{dataset_identifier}</Catalog>
        </PropertyList>
      </Properties>
    </Discover>
  </soap:Body>
</soap:Envelope>"""

            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "text/xml",
                "SOAPAction": "urn:schemas-microsoft-com:xml-analysis:Discover"
            }

            response = requests.post(
                xmla_endpoint,
                headers=headers,
                data=xmla_request,
                timeout=60
            )

            if response.status_code != 200:
                logger.error(f"XMLA request failed: {response.status_code} - {response.text}")
                return []

            # Parse XML response
            import xml.etree.ElementTree as ET
            root = ET.fromstring(response.text)

            # Define namespaces
            namespaces = {
                'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
                's': 'http://schemas.xmlsoap.org/soap/envelope/',
                'return': 'urn:schemas-microsoft-com:xml-analysis:rowset',
                'xsd': 'http://www.w3.org/2001/XMLSchema',
                'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
            }

            # Extract table rows from XML
            tables = []
            rows = root.findall('.//return:row', namespaces)

            for row in rows:
                table_name_elem = row.find('return:Name', namespaces)
                is_hidden_elem = row.find('return:IsHidden', namespaces)
                description_elem = row.find('return:Description', namespaces)

                if table_name_elem is not None:
                    table_name = table_name_elem.text
                    is_hidden = is_hidden_elem.text == 'true' if is_hidden_elem is not None else False

                    # Filter out hidden and system tables
                    if not is_hidden and table_name and not table_name.startswith('$') and not table_name.startswith('DateTableTemplate'):
                        tables.append({
                            "name": table_name,
                            "description": description_elem.text if description_elem is not None and description_elem.text else "No description"
                        })

            logger.info(f"Found {len(tables)} tables via XMLA")
            return tables

        except Exception as e:
            logger.error(f"XMLA discovery failed: {str(e)}")
            import traceback
            logger.debug(traceback.format_exc())
            return []

    def verify_table(self, table_name: str) -> bool:
        """Verify if a table exists by attempting to query it"""
        try:
            # Add quotes if table name contains spaces or special characters
            if ' ' in table_name or '&' in table_name or table_name.startswith('_'):
                quoted_name = f"'{table_name}'"
            else:
                quoted_name = table_name

            dax_query = f"EVALUATE TOPN(1, {quoted_name})"
            result = self.execute_dax(dax_query)
            return "results" in result and len(result["results"]) > 0
        except Exception as e:
            logger.debug(f"Table '{table_name}' verification failed: {str(e)}")
            return False

    def discover_tables_by_sampling(self, common_table_names: List[str] = None) -> List[Dict[str, Any]]:
        """
        Try to discover tables by attempting to query common table names.

        Args:
            common_table_names: List of table names to try. If None, uses default list.

        Returns:
            List of discovered tables
        """
        if common_table_names is None:
            # Actual table names from Salesforce BI dataset (from working implementation)
            common_table_names = [
                'Salesforce_Data',
                'Appointments',
                'Leads',
                'Leads_Journey',
                'Opportunities to Appointments',
                'dim_projects',
                'DateTable',
                '_measures',
                '_measures_L&A',
                'Month_Sort',
                'Designer_Sort',
                'SPL_Sort',
                'Source_Sort',
                'LS Group Sort',
                'Lead Source Channels'
            ]

        logger.info(f"Attempting to discover tables by sampling {len(common_table_names)} common names...")
        discovered_tables = []

        for table_name in common_table_names:
            try:
                if self.verify_table(table_name):
                    logger.info(f"Found table: {table_name}")
                    discovered_tables.append({
                        "name": table_name,
                        "description": "Discovered by sampling"
                    })
            except:
                continue

        logger.info(f"Discovered {len(discovered_tables)} tables by sampling")
        return discovered_tables

    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Get columns for a specific table using multiple fallback methods"""

        # Method 1: Try INFO.COLUMNS() (works in newer datasets)
        try:
            logger.info(f"Trying INFO.COLUMNS() for table '{table_name}'...")
            dax_query = f"""
            EVALUATE
            SELECTCOLUMNS(
                INFO.COLUMNS("{table_name}"),
                "ColumnName", [ExplicitName],
                "DataType", [DataType],
                "Description", [Description]
            )
            """

            result = self.execute_dax(dax_query)

            columns = []
            if "results" in result and len(result["results"]) > 0:
                rows = result["results"][0].get("tables", [{}])[0].get("rows", [])
                for row in rows:
                    columns.append({
                        "name": row.get("ColumnName", ""),
                        "type": row.get("DataType", ""),
                        "description": row.get("Description") or "No description"
                    })

            logger.info(f"Found {len(columns)} columns using INFO.COLUMNS()")
            return {
                "table_name": table_name,
                "columns": columns
            }

        except Exception as e:
            logger.warning(f"INFO.COLUMNS() failed: {str(e)}, trying DMV query...")

        # Method 2: Try DMV query
        try:
            logger.info(f"Trying DMV query for columns in '{table_name}'...")
            dax_query = f"""
            EVALUATE
            SELECTCOLUMNS(
                FILTER(
                    $SYSTEM.TMSCHEMA_COLUMNS,
                    [TABLE_NAME] = "{table_name}"
                ),
                "ColumnName", [COLUMN_NAME],
                "DataType", [DATA_TYPE]
            )
            """

            result = self.execute_dax(dax_query)

            columns = []
            if "results" in result and len(result["results"]) > 0:
                rows = result["results"][0].get("tables", [{}])[0].get("rows", [])
                for row in rows:
                    columns.append({
                        "name": row.get("ColumnName", ""),
                        "type": self._map_data_type(row.get("DataType", "")),
                        "description": "No description"
                    })

            logger.info(f"Found {len(columns)} columns using DMV query")
            return {
                "table_name": table_name,
                "columns": columns
            }

        except Exception as e:
            logger.warning(f"DMV query failed: {str(e)}, trying fallback...")

        # Method 3: Fallback - sample the table to discover columns
        return self._get_schema_fallback(table_name)

    def _map_data_type(self, dmv_type: int) -> str:
        """Map DMV numeric data types to readable names"""
        type_mapping = {
            2: "Integer",
            3: "Double",
            6: "Currency",
            7: "DateTime",
            8: "String",
            11: "Boolean",
            17: "Decimal"
        }
        return type_mapping.get(dmv_type, f"Type_{dmv_type}")

    def _get_schema_fallback(self, table_name: str) -> Dict[str, Any]:
        """Fallback to get schema by sampling the table"""
        try:
            dax_query = f"EVALUATE TOPN(1, '{table_name}')"
            result = self.execute_dax(dax_query)

            columns = []
            if "results" in result and len(result["results"]) > 0:
                table = result["results"][0].get("tables", [{}])[0]
                # Get column names from the first row
                if table.get("rows") and len(table["rows"]) > 0:
                    first_row = table["rows"][0]
                    columns = [{"name": col, "type": "Unknown", "description": ""} for col in first_row.keys()]

            return {
                "table_name": table_name,
                "columns": columns
            }
        except Exception as e:
            logger.error(f"Fallback schema discovery failed: {str(e)}")
            return {"table_name": table_name, "columns": []}

    def get_sample_data(self, table_name: str, num_rows: int = 5) -> List[Dict[str, Any]]:
        """Get sample data from a table"""
        try:
            dax_query = f"EVALUATE TOPN({num_rows}, '{table_name}')"
            result = self.execute_dax(dax_query)

            rows = []
            if "results" in result and len(result["results"]) > 0:
                rows = result["results"][0].get("tables", [{}])[0].get("rows", [])

            return rows

        except Exception as e:
            logger.error(f"Failed to get sample data: {str(e)}")
            return []
