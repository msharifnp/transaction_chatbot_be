import hashlib
import logging
import re
from threading import Lock
from azure.identity import (
    ChainedTokenCredential,
    EnvironmentCredential,
    ManagedIdentityCredential,
)
from azure.keyvault.secrets import SecretClient
from src.config.key_vault_config import KeyVaultConfig

logger = logging.getLogger(__name__)


class AzureKeyVaultService:
    _client = None
    _credential = None
    _lock = Lock()
    _MAX_SECRET_NAME_LENGTH = 127

    def is_configured(self) -> bool:
        return KeyVaultConfig.is_configured()

    def ensure_configured(self) -> None:
        if not self.is_configured():
            raise RuntimeError(
                "Azure Key Vault is not configured. Set AZURE_KEY_VAULT_URL."
            )

        if KeyVaultConfig.has_partial_service_principal_credentials():
            raise RuntimeError(
                "Azure service principal configuration is incomplete. Set all of "
                "AZURE_CLIENT_ID, AZURE_TENANT_ID, and AZURE_CLIENT_SECRET, or remove "
                "the partial values and use managed identity."
            )

    def build_secret_name(self, tenant_id: str, purpose: str) -> str:
        tenant_slug = self._slugify(tenant_id)
        purpose_slug = self._slugify(purpose)
        base_name = f"{tenant_slug}-{purpose_slug}"

        if len(base_name) <= self._MAX_SECRET_NAME_LENGTH:
            return base_name

        digest = hashlib.sha1(base_name.encode("utf-8")).hexdigest()[:12]
        max_base_length = self._MAX_SECRET_NAME_LENGTH - len(digest) - 1
        return f"{base_name[:max_base_length].rstrip('-')}-{digest}"

    def is_managed_secret_ref(
        self,
        tenant_id: str,
        purpose: str,
        credentials_ref: str,
    ) -> bool:
        secret_name = self.extract_secret_name(credentials_ref)
        if not secret_name:
            return False

        return secret_name == self.build_secret_name(tenant_id, purpose)

    def extract_secret_name(self, credentials_ref: str) -> str:
        secret_ref = str(credentials_ref or "").strip()
        if not secret_ref:
            return ""

        if secret_ref.startswith("kv://"):
            return secret_ref[5:].strip("/")

        if secret_ref.startswith("https://") and "/secrets/" in secret_ref:
            return secret_ref.split("/secrets/", 1)[1].split("/", 1)[0].strip()

        vault_url = KeyVaultConfig.get_vault_url(strict=False)
        secret_prefix = f"{vault_url}/secrets/"
        if vault_url and secret_ref.startswith(secret_prefix):
            remainder = secret_ref[len(secret_prefix):]
            return remainder.split("/", 1)[0].strip()

        return secret_ref

    def set_secret(self, secret_name: str, secret_value: str) -> str:
        self.ensure_configured()
        normalized_secret_name = self.extract_secret_name(secret_name)
        normalized_secret_value = str(secret_value or "").strip()

        if not normalized_secret_name:
            raise ValueError("Secret name is required")
        if not normalized_secret_value:
            raise ValueError("Secret value is required")

        logger.info("[KEY VAULT] Setting secret %s", normalized_secret_name)
        self._get_client().set_secret(normalized_secret_name, normalized_secret_value)
        return normalized_secret_name

    def get_secret_value(self, secret_ref: str) -> str:
        self.ensure_configured()
        secret_name = self.extract_secret_name(secret_ref)
        if not secret_name:
            return ""

        logger.info("[KEY VAULT] Resolving secret %s", secret_name)
        secret = self._get_client().get_secret(secret_name)
        return str(secret.value or "").strip()

    def delete_secret(self, secret_ref: str) -> None:
        if not self.is_configured():
            return

        secret_name = self.extract_secret_name(secret_ref)
        if not secret_name:
            return

        logger.info("[KEY VAULT] Deleting secret %s", secret_name)
        self._get_client().begin_delete_secret(secret_name)

    def _get_client(self) -> SecretClient:
        self.ensure_configured()

        with self._lock:
            if self._client is None:
                self._credential = self._build_credential()
                self._client = SecretClient(
                    vault_url=KeyVaultConfig.get_vault_url(),
                    credential=self._credential,
                )

        return self._client

    def _build_credential(self):
        credentials = []

        if KeyVaultConfig.has_service_principal_credentials():
            logger.info("[KEY VAULT] Using Azure service principal credentials from environment")
            credentials.append(EnvironmentCredential())

        managed_identity_client_id = KeyVaultConfig.get_azure_client_id() or None
        if managed_identity_client_id:
            logger.info(
                "[KEY VAULT] Managed identity fallback enabled with client id %s",
                managed_identity_client_id,
            )
        else:
            logger.info("[KEY VAULT] Managed identity fallback enabled")

        credentials.append(ManagedIdentityCredential(client_id=managed_identity_client_id))

        return ChainedTokenCredential(*credentials)

    def _slugify(self, value: str) -> str:
        normalized = re.sub(r"[^0-9A-Za-z-]+", "-", str(value or "").strip().lower())
        normalized = re.sub(r"-{2,}", "-", normalized).strip("-")
        return normalized or "default"
