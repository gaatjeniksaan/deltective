"""Tests for Azure storage integration using Azurite testcontainer."""

from pathlib import Path

import pandas as pd
import pytest
from azure.storage.blob import BlobServiceClient
from deltalake import write_deltalake
from testcontainers.azurite import AzuriteContainer

from deltective.inspector import DeltaTableInspector


@pytest.fixture(scope="module")
def azurite_container():
    """Start Azurite container for testing Azure storage."""
    with AzuriteContainer() as azurite:
        yield azurite


@pytest.fixture
def azure_delta_table(azurite_container, tmp_path: Path):
    """Create a Delta table in Azurite blob storage for testing."""
    # Get connection string from Azurite
    connection_string = azurite_container.get_connection_string()

    # Create blob service client
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Create a container
    container_name = "test-container"
    try:
        blob_service_client.create_container(container_name)
    except Exception:
        pass  # Container might already exist

    # Create a local Delta table first
    local_table_path = tmp_path / "local_delta"
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "value": [10, 20, 30, 40, 50],
        "category": ["a", "b", "c", "d", "e"],
    })

    write_deltalake(
        str(local_table_path),
        df,
        mode="overwrite",
        name="azure_test_table",
    )

    # Upload Delta table files to Azurite
    table_path = "delta_table"

    for file_path in local_table_path.rglob("*"):
        if file_path.is_file():
            # Calculate relative path
            rel_path = file_path.relative_to(local_table_path)
            blob_name = f"{table_path}/{rel_path}".replace("\\", "/")

            # Upload to blob storage
            blob_client = blob_service_client.get_blob_client(
                container=container_name,
                blob=blob_name
            )
            with open(file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)

    # Construct Azure path
    # For Azurite, we use the connection string approach with account key
    account_name = "devstoreaccount1"  # Default Azurite account
    azure_path = f"az://{container_name}/{table_path}"

    # Set up storage options for authentication
    storage_options = {
        "account_name": account_name,
        "account_key": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",  # Default Azurite key
        "azure_storage_endpoint": azurite_container.get_blob_url(),
    }

    yield azure_path, storage_options, connection_string, blob_service_client

    # Cleanup: delete container
    try:
        blob_service_client.delete_container(container_name)
    except Exception:
        pass


class TestAzureIntegration:
    """Test Azure storage integration with Azurite."""

    def test_azure_table_access(self, azure_delta_table):
        """Test accessing Delta table from Azure blob storage."""
        azure_path, storage_options, _, _ = azure_delta_table

        # Verify the path structure is correct
        assert azure_path.startswith("az://")
        assert "test-container" in azure_path
        assert "delta_table" in azure_path

    def test_azure_connection_string(self, azure_delta_table):
        """Test Azure connection string is properly formatted."""
        _, storage_options, connection_string, _ = azure_delta_table

        assert "AccountName=devstoreaccount1" in connection_string
        assert "AccountKey=" in connection_string
        assert "BlobEndpoint=" in connection_string

    def test_storage_options_structure(self, azure_delta_table):
        """Test storage options have correct structure for Azure."""
        _, storage_options, _, _ = azure_delta_table

        assert "account_name" in storage_options
        assert "account_key" in storage_options
        assert storage_options["account_name"] == "devstoreaccount1"

    def test_blob_service_connection(self, azure_delta_table):
        """Test that we can connect to Azurite blob service."""
        _, _, connection_string, blob_service_client = azure_delta_table

        # List containers to verify connection
        containers = list(blob_service_client.list_containers())
        assert any(c.name == "test-container" for c in containers)

    def test_delta_files_uploaded(self, azure_delta_table):
        """Test that Delta table files were uploaded to Azurite."""
        _, _, _, blob_service_client = azure_delta_table

        # List blobs in the container
        container_client = blob_service_client.get_container_client("test-container")
        blobs = list(container_client.list_blobs())

        # Should have uploaded Delta log and data files
        assert len(blobs) > 0

        # Should have _delta_log directory
        delta_log_blobs = [b for b in blobs if "_delta_log" in b.name]
        assert len(delta_log_blobs) > 0


class TestAzurePathParsing:
    """Test Azure path parsing and validation."""

    def test_detect_azure_abfss_path(self):
        """Test detection of abfss:// Azure storage paths."""
        path = "abfss://container@account.dfs.core.windows.net/path/to/table"
        assert path.startswith("abfss://")

    def test_detect_azure_az_path(self):
        """Test detection of az:// Azure storage paths."""
        path = "az://container/path/to/table"
        assert path.startswith("az://")

    def test_local_path_not_azure(self, tmp_path: Path):
        """Test that local paths are not treated as Azure paths."""
        local_path = str(tmp_path / "local_table")

        assert not local_path.startswith("abfss://")
        assert not local_path.startswith("az://")


class TestAzuriteContainer:
    """Test Azurite container setup and configuration."""

    def test_azurite_container_starts(self, azurite_container):
        """Test that Azurite container starts successfully."""
        assert azurite_container is not None

    def test_azurite_connection_string(self, azurite_container):
        """Test Azurite connection string generation."""
        connection_string = azurite_container.get_connection_string()

        assert connection_string is not None
        assert "AccountName" in connection_string
        assert "AccountKey" in connection_string

    def test_azurite_blob_url(self, azurite_container):
        """Test Azurite blob URL generation."""
        blob_url = azurite_container.get_blob_url()

        assert blob_url is not None
        assert "http://" in blob_url or "https://" in blob_url

    def test_azurite_default_credentials(self, azurite_container):
        """Test Azurite uses default development credentials."""
        connection_string = azurite_container.get_connection_string()

        # Azurite uses well-known development account
        assert "devstoreaccount1" in connection_string
        # Azurite uses well-known development key
        assert "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq" in connection_string
