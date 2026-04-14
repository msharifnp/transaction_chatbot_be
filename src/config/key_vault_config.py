import os
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()

class KeyVaultConfig:
    @classmethod
    def is_configured(cls) -> bool:
        return bool(cls.get_vault_url(strict=False))

    @classmethod
    def get_vault_url(cls, strict: bool = True) -> str:
        raw_value = os.getenv("AZURE_KEY_VAULT_URL", "").strip()
        raw_value = raw_value.lstrip("=").strip().strip("'\"")
        if not raw_value:
            return ""

        if "://" not in raw_value:
            raw_value = f"https://{raw_value}"

        parsed = urlparse(raw_value)
        if parsed.scheme.lower() != "https" or not parsed.netloc:
            if not strict:
                return ""
            raise ValueError(
                "AZURE_KEY_VAULT_URL must be a valid https URL, for example "
                "'https://your-vault-name.vault.azure.net/'"
            )

        return raw_value.rstrip("/")

    @classmethod
    def get_azure_client_id(cls) -> str:
        return os.getenv("AZURE_CLIENT_ID", "").strip().strip("'\"")

    @classmethod
    def get_azure_tenant_id(cls) -> str:
        return os.getenv("AZURE_TENANT_ID", "").strip().strip("'\"")

    @classmethod
    def get_azure_client_secret(cls) -> str:
        return os.getenv("AZURE_CLIENT_SECRET", "").strip().strip("'\"")

    @classmethod
    def has_service_principal_credentials(cls) -> bool:
        return bool(
            cls.get_azure_client_id()
            and cls.get_azure_tenant_id()
            and cls.get_azure_client_secret()
        )

    @classmethod
    def has_partial_service_principal_credentials(cls) -> bool:
        client_id = cls.get_azure_client_id()
        tenant_id = cls.get_azure_tenant_id()
        client_secret = cls.get_azure_client_secret()

        # `AZURE_CLIENT_ID` alone is valid for a user-assigned managed identity.
        if client_id and not tenant_id and not client_secret:
            return False

        values = [client_id, tenant_id, client_secret]
        return any(values) and not all(values)
