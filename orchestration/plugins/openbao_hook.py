from __future__ import annotations

import os
from enum import Enum
from typing import Optional

import hvac
from airflow.hooks.base import BaseHook
from airflow.models import Variable


class OpenBaoAuthMethod(str, Enum):
    TOKEN    = "token"
    USERPASS = "userpass"


class OpenBaoHook(BaseHook):
    """
    Airflow hook for reading secrets from OpenBao (Vault-compatible API).

    Supports two authentication methods, resolved in this priority order:

    1. **Token** — a static root or AppRole token.
    2. **Username / Password** — userpass auth method.

    Auth method and credentials are read from **Airflow Variables** (set via
    Admin → Variables in the Airflow UI). Environment variables are used as a
    fallback when a Variable is not set.

    Airflow Variables
    -----------------
    Set these in the Airflow UI under Admin → Variables:

    ``openbao_auth_method``
        ``token`` (default) or ``userpass``.

    ``openbao_token``
        Token used when auth method is ``token``.

    ``openbao_username``
        Username used when auth method is ``userpass``.

    ``openbao_password``
        Password used when auth method is ``userpass``.

    Environment variable fallbacks
    ------------------------------
    If an Airflow Variable is not set, the hook falls back to the
    corresponding environment variable:

    =====================  =======================
    Airflow Variable       Environment variable
    =====================  =======================
    openbao_auth_method    OPENBAO_AUTH_METHOD
    openbao_token          OPENBAO_TOKEN
    openbao_username       OPENBAO_USERNAME
    openbao_password       OPENBAO_PASSWORD
    =====================  =======================

    Airflow connection (``openbao_default``)
    ----------------------------------------
    Configure via Admin → Connections in the Airflow UI or CLI:

    .. code-block:: bash

        # Token auth — store token in Variable, connection just needs host/port
        airflow connections add openbao_default \\
            --conn-type http \\
            --conn-host http://openbao.infra.svc.cluster.local \\
            --conn-port 8200

        # Userpass auth — same connection, credentials in Variables
        airflow connections add openbao_default \\
            --conn-type http \\
            --conn-host http://openbao.infra.svc.cluster.local \\
            --conn-port 8200 \\
            --conn-schema userpass          # mount path, defaults to 'userpass'

    Connection fields used
    ----------------------
    host     : OpenBao base URL (e.g. ``http://openbao:8200``)
    port     : OpenBao port — appended to host when host has no port
    schema   : Userpass mount path (userpass auth only, defaults to ``userpass``)

    Usage
    -----
    >>> hook = OpenBaoHook()
    >>> creds = hook.get_secret("data-processor/postgres")
    >>> # {"host": "pg-host", "port": "5432", "username": "...", "password": "..."}
    """

    conn_name_attr    = "openbao_conn_id"
    default_conn_name = "openbao_default"
    conn_type         = "http"
    hook_name         = "OpenBao"

    # Airflow Variable keys
    _VAR_AUTH_METHOD = "openbao_auth_method"
    _VAR_TOKEN       = "openbao_token"
    _VAR_USERNAME    = "openbao_username"
    _VAR_PASSWORD    = "openbao_password"

    # Environment variable fallback keys
    _ENV_AUTH_METHOD = "OPENBAO_AUTH_METHOD"
    _ENV_TOKEN       = "OPENBAO_TOKEN"
    _ENV_USERNAME    = "OPENBAO_USERNAME"
    _ENV_PASSWORD    = "OPENBAO_PASSWORD"

    def __init__(self, openbao_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.openbao_conn_id = openbao_conn_id
        self._client: Optional[hvac.Client] = None

    # ── Public interface ──────────────────────────────────────────────────

    def get_conn(self) -> hvac.Client:
        """
        Return an authenticated hvac.Client.
        Cached for the lifetime of this hook instance.
        Re-authenticates automatically if the cached client expires.
        """
        if self._client and self._client.is_authenticated():
            return self._client
        self._client = self._authenticate()
        return self._client

    def get_secret(self, secret_path: str) -> dict:
        """
        Read a KV v2 secret from OpenBao and return its data dict.

        Parameters
        ----------
        secret_path : str
            Path relative to the KV v2 engine mount (``secret/``).
            Example: ``"data-processor/postgres"``
            resolves to ``secret/data/data-processor/postgres``.

        Returns
        -------
        dict
            Key-value pairs stored at the secret path.

        Raises
        ------
        RuntimeError
            If authentication fails or the secret path does not exist.
        """
        client = self.get_conn()
        response = client.secrets.kv.v2.read_secret_version(
            path=secret_path,
            mount_point="secret",
        )
        return response["data"]["data"]

    # ── Authentication ────────────────────────────────────────────────────

    def _authenticate(self) -> hvac.Client:
        """
        Build and authenticate an hvac.Client based on the configured
        auth method (Airflow Variable → env var fallback).
        """
        url    = self._resolve_url()
        method = self._resolve_auth_method()
        client = hvac.Client(url=url)

        if method == OpenBaoAuthMethod.TOKEN:
            self._auth_token(client)
        elif method == OpenBaoAuthMethod.USERPASS:
            self._auth_userpass(client)
        else:
            raise ValueError(
                f"Unsupported auth method: '{method}'. "
                f"Valid options: {[m.value for m in OpenBaoAuthMethod]}"
            )

        if not client.is_authenticated():
            raise RuntimeError(
                f"OpenBao authentication failed using method '{method}' at '{url}'"
            )

        return client

    def _auth_token(self, client: hvac.Client) -> None:
        """
        Set a static token on the client.
        Priority: Airflow Variable 'openbao_token' → OPENBAO_TOKEN env var.
        """
        token = self._get_var(self._VAR_TOKEN, self._ENV_TOKEN)
        if not token:
            raise RuntimeError(
                "Token auth requires the Airflow Variable 'openbao_token' "
                "or the OPENBAO_TOKEN environment variable to be set."
            )
        client.token = token

    def _auth_userpass(self, client: hvac.Client) -> None:
        """
        Authenticate via the userpass auth method.
        Priority for each field: Airflow Variable → env var fallback.
        Mount path comes from the Airflow connection schema field
        (defaults to 'userpass').
        """
        username = self._get_var(self._VAR_USERNAME, self._ENV_USERNAME)
        password = self._get_var(self._VAR_PASSWORD, self._ENV_PASSWORD)
        mount    = self._conn_schema() or "userpass"

        if not username or not password:
            raise RuntimeError(
                "Userpass auth requires the Airflow Variables 'openbao_username' "
                "and 'openbao_password' (or their OPENBAO_USERNAME / "
                "OPENBAO_PASSWORD environment variable fallbacks) to be set."
            )

        client.auth.userpass.login(
            username=username,
            password=password,
            mount_point=mount,
        )

    # ── Variable resolution ───────────────────────────────────────────────

    @staticmethod
    def _get_var(variable_key: str, env_fallback: str) -> str:
        """
        Read a value from Airflow Variables with an env var as fallback.

        Resolution order:
        1. Airflow Variable (Admin → Variables in the UI)
        2. Environment variable
        3. Empty string (caller decides whether to raise)

        Parameters
        ----------
        variable_key : str
            The Airflow Variable key to look up.
        env_fallback : str
            The environment variable name to use when the Variable is not set.
        """
        try:
            value = Variable.get(variable_key, default_var=None)
            if value is not None:
                return value
        except Exception:
            # Variable backend unavailable (e.g. during unit tests without DB)
            pass
        return os.environ.get(env_fallback, "")

    def _resolve_url(self) -> str:
        """
        Build the OpenBao URL from the Airflow connection host and port.
        If host already contains a port the connection port field is ignored.
        """
        conn = self.get_connection(self.openbao_conn_id)
        host = conn.host or "http://localhost"
        port = conn.port
        if port and ":" not in host.split("//")[-1]:
            return f"{host}:{port}"
        return host

    def _resolve_auth_method(self) -> OpenBaoAuthMethod:
        """
        Resolve auth method.
        Priority: Airflow Variable 'openbao_auth_method' → OPENBAO_AUTH_METHOD env var.
        Defaults to TOKEN if unset or unrecognised.
        """
        raw = self._get_var(self._VAR_AUTH_METHOD, self._ENV_AUTH_METHOD)
        raw = raw.lower() if raw else OpenBaoAuthMethod.TOKEN.value
        try:
            return OpenBaoAuthMethod(raw)
        except ValueError:
            return OpenBaoAuthMethod.TOKEN

    def _conn_schema(self) -> str:
        return self.get_connection(self.openbao_conn_id).schema or ""
