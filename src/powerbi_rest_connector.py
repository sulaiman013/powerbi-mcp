"""
Power BI REST API Connector
For listing workspaces and datasets from Power BI Service
"""
import logging
from typing import Any, Dict, List, Optional
import requests
import msal

logger = logging.getLogger(__name__)


class PowerBIRestConnector:
    """Power BI connector using REST API for workspace/dataset listing"""

    BASE_URL = "https://api.powerbi.com/v1.0/myorg"
    AUTHORITY = "https://login.microsoftonline.com/{tenant_id}"
    SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]

    def __init__(self, tenant_id: str, client_id: str, client_secret: str):
        """Initialize connector with Azure AD credentials"""
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None

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
                logger.info("Successfully authenticated to Power BI Service")
                return True
            else:
                error = result.get("error_description", "Unknown error")
                logger.error(f"Authentication failed: {error}")
                return False

        except Exception as e:
            logger.error(f"Authentication error: {str(e)}")
            return False

    def _get_headers(self) -> Dict[str, str]:
        """Get HTTP headers with authorization"""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    def list_workspaces(self) -> List[Dict[str, Any]]:
        """
        List all workspaces accessible by the Service Principal
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
                }
                for ws in workspaces
            ]

        except Exception as e:
            logger.error(f"Failed to list workspaces: {str(e)}")
            return []

    def list_datasets(self, workspace_id: str) -> List[Dict[str, Any]]:
        """
        List all datasets in a workspace
        """
        try:
            if not self.access_token:
                if not self.authenticate():
                    return []

            url = f"{self.BASE_URL}/groups/{workspace_id}/datasets"
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
                }
                for ds in datasets
            ]

        except Exception as e:
            logger.error(f"Failed to list datasets: {str(e)}")
            return []
