"""
Kubernetes secrets reader.
Reads secrets directly from the Kubernetes API.
"""

import base64
from typing import Optional
from kubernetes import client, config


class K8sSecretsReader:
    """
    Read secrets directly from Kubernetes API.
    """
    _instance: Optional['K8sSecretsReader'] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._initialized = True
        # Load in-cluster config (when running inside K8s)
        # Falls back to kubeconfig for local development
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()

        self._v1 = client.CoreV1Api()
        self._cache = {}

    def get_secret(self, secret_name: str, namespace: str, key: str) -> bytes:
        """
        Get a specific key from a Kubernetes secret.

        Args:
            secret_name: Name of the secret
            namespace: Namespace where the secret is located
            key: Key within the secret data

        Returns:
            The secret value as bytes

        Raises:
            Exception: If secret or key not found
        """
        cache_key = f"{namespace}/{secret_name}"

        # Check cache first
        if cache_key not in self._cache:
            secret = self._v1.read_namespaced_secret(secret_name, namespace)
            self._cache[cache_key] = secret.data

        secret_data = self._cache[cache_key]
        if key not in secret_data:
            raise KeyError(f"Key '{key}' not found in secret '{secret_name}'")

        # Kubernetes secrets are base64 encoded
        return base64.b64decode(secret_data[key])

    def clear_cache(self):
        """Clear the secret cache."""
        self._cache = {}


# Global instance
k8s_secrets_reader = K8sSecretsReader()


def read_cert_from_k8s_secret(secret_name: str, namespace: str, key: str) -> bytes:
    """
    Convenience function to read a certificate from a Kubernetes secret.

    Args:
        secret_name: Name of the secret
        namespace: Namespace where the secret is located
        key: Key within the secret data (e.g., 'client.key', 'client.crt', 'ca.crt')

    Returns:
        The certificate as bytes
    """
    return k8s_secrets_reader.get_secret(secret_name, namespace, key)
