from __future__ import annotations

import hvac
from airflow.hooks.base import BaseHook


class OpenBaoHook(BaseHook):
    """
    Airflow hook for reading secrets from OpenBao (Vault-compatible API).

    Authenticates using the Kubernetes auth method — the Airflow scheduler
    pod's service account token is exchanged for a scoped OpenBao token
    at runtime. No static tokens are stored in Airflow connections.

    The OpenBao connection must be configured in Airflow Connections with:
        conn_id   : openbao_default  (or custom)
        conn_type : HTTP
        host      : OpenBao service URL  e.g. http://openbao.infra.svc.cluster.local
        port      : 8200
        schema    : Kubernetes auth role  e.g. airflow

    Usage
    -----
    hook = OpenBaoHook()
    creds = hook.get_secret("data-processor/postgres")
    # returns {"username": "...", "password": "..."}
    """

    conn_name_attr = "openbao_conn_id"
    default_conn_name = "openbao_default"
    conn_type = "http"
    hook_name = "OpenBao"

    # Path to the Kubernetes service account token mounted in the pod
    _K8S_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"

    def __init__(self, openbao_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.openbao_conn_id = openbao_conn_id
        self._client: hvac.Client | None = None

    def get_conn(self) -> hvac.Client:
        """
        Authenticate against OpenBao using the Kubernetes auth method.
        Returns an authenticated hvac.Client. Caches client for the hook lifetime.
        """
        if self._client and self._client.is_authenticated():
            return self._client

        conn = self.get_connection(self.openbao_conn_id)
        url = f"{conn.host}:{conn.port}"
        role = conn.schema or "airflow"

        with open(self._K8S_TOKEN_PATH) as f:
            jwt_token = f.read().strip()

        client = hvac.Client(url=url)
        client.auth.kubernetes.login(role=role, jwt=jwt_token)

        if not client.is_authenticated():
            raise RuntimeError(
                f"OpenBao authentication failed for role '{role}' at '{url}'"
            )

        self._client = client
        return self._client

    def get_secret(self, secret_path: str) -> dict:
        """
        Read a KV v2 secret from OpenBao.

        Parameters
        ----------
        secret_path : str
            Path relative to the KV engine mount, e.g.
            'data-processor/postgres' → secret/data/data-processor/postgres

        Returns
        -------
        dict
            The secret's key-value data dict.
        """
        client = self.get_conn()
        response = client.secrets.kv.v2.read_secret_version(
            path=secret_path,
            mount_point="secret",
        )
        return response["data"]["data"]
