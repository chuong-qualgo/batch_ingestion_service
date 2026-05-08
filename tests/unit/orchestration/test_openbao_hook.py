"""
Unit tests for OpenBaoHook.

All hvac.Client calls and Airflow Variable lookups are mocked.
No real OpenBao instance or Airflow database is needed.

Resolution priority under test:
  Airflow Variable → env var fallback → empty string
"""
import pytest
from unittest.mock import MagicMock, patch, call

from orchestration.plugins.openbao_hook import OpenBaoHook, OpenBaoAuthMethod


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_mock_conn(
    host="http://openbao",
    port=8200,
    schema="userpass",
):
    conn = MagicMock()
    conn.host   = host
    conn.port   = port
    conn.schema = schema
    return conn


def make_hook(conn=None):
    hook = OpenBaoHook()
    hook.get_connection = MagicMock(return_value=conn or make_mock_conn())
    return hook


def no_variable(key, default_var=None):
    """Simulate Variable.get when no variable is set."""
    return default_var


def variable_map(**kwargs):
    """Return a side_effect function that maps keys to values."""
    def _get(key, default_var=None):
        return kwargs.get(key, default_var)
    return _get


# ── _get_var ──────────────────────────────────────────────────────────────────

class TestGetVar:

    def test_returns_airflow_variable_when_set(self):
        with patch("orchestration.plugins.openbao_hook.Variable.get",
                   side_effect=variable_map(openbao_token="from-variable")):
            result = OpenBaoHook._get_var("openbao_token", "OPENBAO_TOKEN")
        assert result == "from-variable"

    def test_falls_back_to_env_when_variable_not_set(self, monkeypatch):
        monkeypatch.setenv("OPENBAO_TOKEN", "from-env")
        with patch("orchestration.plugins.openbao_hook.Variable.get",
                   side_effect=no_variable):
            result = OpenBaoHook._get_var("openbao_token", "OPENBAO_TOKEN")
        assert result == "from-env"

    def test_variable_takes_priority_over_env(self, monkeypatch):
        monkeypatch.setenv("OPENBAO_TOKEN", "env-value")
        with patch("orchestration.plugins.openbao_hook.Variable.get",
                   side_effect=variable_map(openbao_token="variable-wins")):
            result = OpenBaoHook._get_var("openbao_token", "OPENBAO_TOKEN")
        assert result == "variable-wins"

    def test_returns_empty_string_when_both_absent(self, monkeypatch):
        monkeypatch.delenv("OPENBAO_TOKEN", raising=False)
        with patch("orchestration.plugins.openbao_hook.Variable.get",
                   side_effect=no_variable):
            result = OpenBaoHook._get_var("openbao_token", "OPENBAO_TOKEN")
        assert result == ""

    def test_falls_back_to_env_when_variable_backend_raises(self, monkeypatch):
        monkeypatch.setenv("OPENBAO_TOKEN", "env-fallback")
        with patch("orchestration.plugins.openbao_hook.Variable.get",
                   side_effect=Exception("DB not available")):
            result = OpenBaoHook._get_var("openbao_token", "OPENBAO_TOKEN")
        assert result == "env-fallback"


# ── _resolve_url ──────────────────────────────────────────────────────────────

class TestResolveUrl:

    def test_host_without_port_appends_port(self):
        hook = make_hook(make_mock_conn(host="http://openbao", port=8200))
        assert hook._resolve_url() == "http://openbao:8200"

    def test_host_with_port_ignores_conn_port(self):
        hook = make_hook(make_mock_conn(host="http://openbao:9999", port=8200))
        assert hook._resolve_url() == "http://openbao:9999"

    def test_host_no_port_field(self):
        hook = make_hook(make_mock_conn(host="http://openbao", port=None))
        assert hook._resolve_url() == "http://openbao"

    def test_fallback_to_localhost(self):
        hook = make_hook(make_mock_conn(host=None, port=None))
        assert hook._resolve_url() == "http://localhost"


# ── _resolve_auth_method ──────────────────────────────────────────────────────

class TestResolveAuthMethod:

    def test_defaults_to_token_when_nothing_set(self, monkeypatch):
        monkeypatch.delenv("OPENBAO_AUTH_METHOD", raising=False)
        hook = make_hook()
        with patch("orchestration.plugins.openbao_hook.Variable.get",
                   side_effect=no_variable):
            assert hook._resolve_auth_method() == OpenBaoAuthMethod.TOKEN

    def test_token_from_airflow_variable(self):
        hook = make_hook()
        with patch("orchestration.plugins.openbao_hook.Variable.get",
                   side_effect=variable_map(openbao_auth_method="token")):
            assert hook._resolve_auth_method() == OpenBaoAuthMethod.TOKEN

    def test_userpass_from_airflow_variable(self):
        hook = make_hook()
        with patch("orchestration.plugins.openbao_hook.Variable.get",
                   side_effect=variable_map(openbao_auth_method="userpass")):
            assert hook._resolve_auth_method() == OpenBaoAuthMethod.USERPASS

    def test_airflow_variable_takes_priority_over_env(self, monkeypatch):
        monkeypatch.setenv("OPENBAO_AUTH_METHOD", "token")
        hook = make_hook()
        with patch("orchestration.plugins.openbao_hook.Variable.get",
                   side_effect=variable_map(openbao_auth_method="userpass")):
            assert hook._resolve_auth_method() == OpenBaoAuthMethod.USERPASS

    def test_falls_back_to_env_when_variable_not_set(self, monkeypatch):
        monkeypatch.setenv("OPENBAO_AUTH_METHOD", "userpass")
        hook = make_hook()
        with patch("orchestration.plugins.openbao_hook.Variable.get",
                   side_effect=no_variable):
            assert hook._resolve_auth_method() == OpenBaoAuthMethod.USERPASS

    def test_unknown_method_defaults_to_token(self):
        hook = make_hook()
        with patch("orchestration.plugins.openbao_hook.Variable.get",
                   side_effect=variable_map(openbao_auth_method="kubernetes")):
            assert hook._resolve_auth_method() == OpenBaoAuthMethod.TOKEN

    def test_case_insensitive(self):
        hook = make_hook()
        with patch("orchestration.plugins.openbao_hook.Variable.get",
                   side_effect=variable_map(openbao_auth_method="USERPASS")):
            assert hook._resolve_auth_method() == OpenBaoAuthMethod.USERPASS


# ── Token auth ────────────────────────────────────────────────────────────────

class TestTokenAuth:

    def test_token_from_airflow_variable(self):
        hook = make_hook()
        client = MagicMock()
        with patch("orchestration.plugins.openbao_hook.Variable.get",
                   side_effect=variable_map(openbao_token="var-token")):
            hook._auth_token(client)
        assert client.token == "var-token"

    def test_token_from_env_when_variable_not_set(self, monkeypatch):
        monkeypatch.setenv("OPENBAO_TOKEN", "env-token")
        hook = make_hook()
        client = MagicMock()
        with patch("orchestration.plugins.openbao_hook.Variable.get",
                   side_effect=no_variable):
            hook._auth_token(client)
        assert client.token == "env-token"

    def test_variable_takes_priority_over_env(self, monkeypatch):
        monkeypatch.setenv("OPENBAO_TOKEN", "env-token")
        hook = make_hook()
        client = MagicMock()
        with patch("orchestration.plugins.openbao_hook.Variable.get",
                   side_effect=variable_map(openbao_token="var-wins")):
            hook._auth_token(client)
        assert client.token == "var-wins"

    def test_raises_when_neither_variable_nor_env_set(self, monkeypatch):
        monkeypatch.delenv("OPENBAO_TOKEN", raising=False)
        hook = make_hook()
        client = MagicMock()
        with patch("orchestration.plugins.openbao_hook.Variable.get",
                   side_effect=no_variable):
            with pytest.raises(RuntimeError, match="openbao_token"):
                hook._auth_token(client)


# ── Userpass auth ─────────────────────────────────────────────────────────────

class TestUserpassAuth:

    def test_userpass_from_airflow_variables(self):
        hook = make_hook(make_mock_conn(schema="userpass"))
        client = MagicMock()
        with patch("orchestration.plugins.openbao_hook.Variable.get",
                   side_effect=variable_map(
                       openbao_username="admin",
                       openbao_password="secret",
                   )):
            hook._auth_userpass(client)
        client.auth.userpass.login.assert_called_once_with(
            username="admin", password="secret", mount_point="userpass"
        )

    def test_username_from_env_fallback(self, monkeypatch):
        monkeypatch.setenv("OPENBAO_USERNAME", "env-user")
        monkeypatch.delenv("OPENBAO_PASSWORD", raising=False)
        hook = make_hook()
        client = MagicMock()
        with patch("orchestration.plugins.openbao_hook.Variable.get",
                   side_effect=variable_map(openbao_password="var-pass")):
            hook._auth_userpass(client)
        client.auth.userpass.login.assert_called_once_with(
            username="env-user", password="var-pass", mount_point="userpass"
        )

    def test_password_from_env_fallback(self, monkeypatch):
        monkeypatch.delenv("OPENBAO_USERNAME", raising=False)
        monkeypatch.setenv("OPENBAO_PASSWORD", "env-pass")
        hook = make_hook()
        client = MagicMock()
        with patch("orchestration.plugins.openbao_hook.Variable.get",
                   side_effect=variable_map(openbao_username="var-user")):
            hook._auth_userpass(client)
        client.auth.userpass.login.assert_called_once_with(
            username="var-user", password="env-pass", mount_point="userpass"
        )

    def test_variable_takes_priority_over_env_for_username(self, monkeypatch):
        monkeypatch.setenv("OPENBAO_USERNAME", "env-user")
        hook = make_hook()
        client = MagicMock()
        with patch("orchestration.plugins.openbao_hook.Variable.get",
                   side_effect=variable_map(
                       openbao_username="var-wins",
                       openbao_password="pass",
                   )):
            hook._auth_userpass(client)
        client.auth.userpass.login.assert_called_once_with(
            username="var-wins", password="pass", mount_point="userpass"
        )

    def test_custom_mount_from_conn_schema(self):
        hook = make_hook(make_mock_conn(schema="my-mount"))
        client = MagicMock()
        with patch("orchestration.plugins.openbao_hook.Variable.get",
                   side_effect=variable_map(
                       openbao_username="u",
                       openbao_password="p",
                   )):
            hook._auth_userpass(client)
        client.auth.userpass.login.assert_called_once_with(
            username="u", password="p", mount_point="my-mount"
        )

    def test_default_mount_when_schema_empty(self):
        hook = make_hook(make_mock_conn(schema=""))
        client = MagicMock()
        with patch("orchestration.plugins.openbao_hook.Variable.get",
                   side_effect=variable_map(
                       openbao_username="u",
                       openbao_password="p",
                   )):
            hook._auth_userpass(client)
        client.auth.userpass.login.assert_called_once_with(
            username="u", password="p", mount_point="userpass"
        )

    def test_raises_when_username_missing(self, monkeypatch):
        monkeypatch.delenv("OPENBAO_USERNAME", raising=False)
        hook = make_hook()
        client = MagicMock()
        with patch("orchestration.plugins.openbao_hook.Variable.get",
                   side_effect=variable_map(openbao_password="p")):
            with pytest.raises(RuntimeError, match="openbao_username"):
                hook._auth_userpass(client)

    def test_raises_when_password_missing(self, monkeypatch):
        monkeypatch.delenv("OPENBAO_PASSWORD", raising=False)
        hook = make_hook()
        client = MagicMock()
        with patch("orchestration.plugins.openbao_hook.Variable.get",
                   side_effect=variable_map(openbao_username="u")):
            with pytest.raises(RuntimeError, match="openbao_password"):
                hook._auth_userpass(client)


# ── get_conn ──────────────────────────────────────────────────────────────────

class TestGetConn:

    def test_returns_authenticated_client_token(self):
        hook = make_hook()
        with patch("orchestration.plugins.openbao_hook.Variable.get",
                   side_effect=variable_map(
                       openbao_auth_method="token",
                       openbao_token="my-token",
                   )), patch("hvac.Client") as mock_cls:
            mock_client = MagicMock()
            mock_client.is_authenticated.return_value = True
            mock_cls.return_value = mock_client
            result = hook.get_conn()
        assert result is mock_client
        assert mock_client.token == "my-token"

    def test_returns_authenticated_client_userpass(self):
        hook = make_hook(make_mock_conn(schema="userpass"))
        with patch("orchestration.plugins.openbao_hook.Variable.get",
                   side_effect=variable_map(
                       openbao_auth_method="userpass",
                       openbao_username="admin",
                       openbao_password="pass",
                   )), patch("hvac.Client") as mock_cls:
            mock_client = MagicMock()
            mock_client.is_authenticated.return_value = True
            mock_cls.return_value = mock_client
            result = hook.get_conn()
        mock_client.auth.userpass.login.assert_called_once()
        assert result is mock_client

    def test_raises_when_not_authenticated(self):
        hook = make_hook()
        with patch("orchestration.plugins.openbao_hook.Variable.get",
                   side_effect=variable_map(
                       openbao_auth_method="token",
                       openbao_token="bad",
                   )), patch("hvac.Client") as mock_cls:
            mock_client = MagicMock()
            mock_client.is_authenticated.return_value = False
            mock_cls.return_value = mock_client
            with pytest.raises(RuntimeError, match="authentication failed"):
                hook.get_conn()

    def test_reuses_cached_authenticated_client(self):
        hook = make_hook()
        cached = MagicMock()
        cached.is_authenticated.return_value = True
        hook._client = cached
        result = hook.get_conn()
        assert result is cached

    def test_reauthenticates_when_cached_client_expired(self):
        hook = make_hook()
        stale = MagicMock()
        stale.is_authenticated.return_value = False
        hook._client = stale
        with patch("orchestration.plugins.openbao_hook.Variable.get",
                   side_effect=variable_map(
                       openbao_auth_method="token",
                       openbao_token="token",
                   )), patch("hvac.Client") as mock_cls:
            fresh = MagicMock()
            fresh.is_authenticated.return_value = True
            mock_cls.return_value = fresh
            result = hook.get_conn()
        assert result is fresh


# ── get_secret ────────────────────────────────────────────────────────────────

class TestGetSecret:

    def test_returns_secret_data(self):
        hook = make_hook()
        mock_client = MagicMock()
        mock_client.is_authenticated.return_value = True
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            "data": {"data": {"host": "pg-host", "username": "admin"}}
        }
        hook._client = mock_client
        result = hook.get_secret("data-processor/postgres")
        assert result == {"host": "pg-host", "username": "admin"}

    def test_calls_correct_path_and_mount(self):
        hook = make_hook()
        mock_client = MagicMock()
        mock_client.is_authenticated.return_value = True
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            "data": {"data": {"k": "v"}}
        }
        hook._client = mock_client
        hook.get_secret("data-platform/hadoop")
        mock_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            path="data-platform/hadoop",
            mount_point="secret",
        )

    def test_propagates_hvac_exception(self):
        hook = make_hook()
        mock_client = MagicMock()
        mock_client.is_authenticated.return_value = True
        mock_client.secrets.kv.v2.read_secret_version.side_effect = Exception("not found")
        hook._client = mock_client
        with pytest.raises(Exception, match="not found"):
            hook.get_secret("nonexistent/path")
