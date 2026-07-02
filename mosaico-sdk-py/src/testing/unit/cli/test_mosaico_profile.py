import pytest

from mosaicolabs_cli.utils.mosaico_profile import MosaicoProfile


class TestNormalizeHostPort:
    def test_plain_host_no_port(self):
        h, p = MosaicoProfile._normalize_host_port("myhost.com")
        assert h == "myhost.com"
        assert p == 6726

    def test_plain_host_with_port_arg(self):
        h, p = MosaicoProfile._normalize_host_port("myhost.com", 9999)
        assert h == "myhost.com"
        assert p == 9999

    def test_host_with_embedded_port(self):
        h, p = MosaicoProfile._normalize_host_port("myhost.com:9999")
        assert h == "myhost.com"
        assert p == 9999

    def test_host_with_embedded_and_explicit_port_raises(self):
        with pytest.raises(ValueError, match="Port given twice"):
            MosaicoProfile._normalize_host_port("myhost.com:9999", 1234)

    def test_malformed_embedded_port_raises(self):
        with pytest.raises(ValueError, match="Malformed input host"):
            MosaicoProfile._normalize_host_port("myhost.com:notaport")

    def test_strips_http_prefix(self):
        h, p = MosaicoProfile._normalize_host_port("http://example.com")
        assert h == "example.com"
        assert p == 6726

    def test_strips_https_prefix(self):
        h, p = MosaicoProfile._normalize_host_port("https://example.com")
        assert h == "example.com"
        assert p == 6726

    def test_strips_trailing_slash(self):
        h, p = MosaicoProfile._normalize_host_port("example.com/")
        assert h == "example.com"
        assert p == 6726

    def test_strips_whitespace(self):
        h, p = MosaicoProfile._normalize_host_port("  example.com  ")
        assert h == "example.com"
        assert p == 6726

    def test_http_with_embedded_port(self):
        h, p = MosaicoProfile._normalize_host_port("http://example.com:8080")
        assert h == "example.com"
        assert p == 8080

    def test_https_with_embedded_port(self):
        h, p = MosaicoProfile._normalize_host_port("https://example.com:443")
        assert h == "example.com"
        assert p == 443


class TestFromDict:
    def test_full_dict(self):
        data = {
            "host": "prod.example.com",
            "port": 8080,
            "api_key": "secret123",
            "tls": True,
            "cert_path": "/certs/ca.pem",
        }
        profile = MosaicoProfile.from_dict(data)
        assert profile.host == "prod.example.com"
        assert profile.port == 8080
        assert profile.api_key == "secret123"
        assert profile.tls is True
        assert profile.cert_path == "/certs/ca.pem"

    def test_minimal_dict(self):
        data = {"host": "localhost"}
        profile = MosaicoProfile.from_dict(data)
        assert profile.host == "localhost"
        assert profile.port == 6726
        assert profile.api_key == ""
        assert profile.tls is False
        assert profile.cert_path == ""

    def test_empty_dict(self):
        profile = MosaicoProfile.from_dict({})
        assert profile.host == ""
        assert profile.port == 6726


class TestToDict:
    def test_roundtrip(self):
        profile = MosaicoProfile(
            host="myhost", port=7777, api_key="key", tls=True, cert_path="/x"
        )
        d = profile.to_dict()
        assert d["host"] == "myhost"
        assert d["port"] == 7777
        assert d["api_key"] == "key"
        assert d["tls"] is True
        assert d["cert_path"] == "/x"


class TestProperties:
    def test_enable_tls_true(self):
        profile = MosaicoProfile(host="h", tls=True)
        assert profile.enable_tls is True

    def test_enable_tls_false(self):
        profile = MosaicoProfile(host="h", tls=False)
        assert profile.enable_tls is False

    def test_tls_cert_path_set(self):
        profile = MosaicoProfile(host="h", cert_path="/some/path")
        assert profile.tls_cert_path == "/some/path"

    def test_tls_cert_path_empty(self):
        profile = MosaicoProfile(host="h", cert_path="")
        assert profile.tls_cert_path is None


class TestToEnv:
    def test_full_profile(self):
        profile = MosaicoProfile(
            host="example.com", port=9999, api_key="k", tls=True, cert_path="/c"
        )
        env = profile.to_env()
        assert env["MOSAICO_DAEMON_URL"] == "example.com:9999"
        assert env["MOSAICO_API_KEY"] == "k"
        assert env["MOSAICO_TLS"] == "true"
        assert env["MOSAICO_CERT_PATH"] == "/c"

    def test_minimal_profile(self):
        profile = MosaicoProfile(host="h", port=6276)
        env = profile.to_env()
        assert "MOSAICO_DAEMON_URL" in env
        assert "MOSAICO_API_KEY" not in env
        assert "MOSAICO_TLS" not in env
        assert "MOSAICO_CERT_PATH" not in env


class TestResolve:
    def test_resolve_from_env(self, monkeypatch, tmp_path):
        config_path = tmp_path / "config.toml"
        monkeypatch.setenv("MOSAICO_CONFIG_PATH", str(config_path))
        monkeypatch.setenv("MOSAICO_DAEMON_URL", "envhost.com:1234")
        monkeypatch.setenv("MOSAICO_API_KEY", "envkey")
        monkeypatch.setenv("MOSAICO_TLS", "true")
        monkeypatch.setenv("MOSAICO_CERT_PATH", "/env/cert.pem")

        profile = MosaicoProfile.resolve()
        assert profile.host == "envhost.com"
        assert profile.port == 1234
        assert profile.api_key == "envkey"
        assert profile.tls is True
        assert profile.cert_path == "/env/cert.pem"

    def test_resolve_from_config_default(self, monkeypatch, tmp_path):
        config_path = tmp_path / "config.toml"
        config_path.write_text(
            '[myprofile]\nhost = "cfghost.com"\nport = "7777"\n'
            'api_key = "cfgkey"\ntls = true\ncert_path = "/cfg/c.pem"\ndefault = true\n'
        )
        monkeypatch.setenv("MOSAICO_CONFIG_PATH", str(config_path))
        monkeypatch.delenv("MOSAICO_DAEMON_URL", raising=False)
        monkeypatch.delenv("MOSAICO_API_KEY", raising=False)
        monkeypatch.delenv("MOSAICO_TLS", raising=False)
        monkeypatch.delenv("MOSAICO_CERT_PATH", raising=False)

        profile = MosaicoProfile.resolve()
        assert profile.host == "cfghost.com"
        assert profile.port == 7777
        assert profile.api_key == "cfgkey"

    def test_resolve_named_profile(self, monkeypatch, tmp_path):
        config_path = tmp_path / "config.toml"
        config_path.write_text(
            '[dev]\nhost = "dev.local"\nport = "6276"\ndefault = true\n\n'
            '[staging]\nhost = "staging.remote"\nport = "8080"\ndefault = false\n'
        )
        monkeypatch.setenv("MOSAICO_CONFIG_PATH", str(config_path))
        monkeypatch.delenv("MOSAICO_DAEMON_URL", raising=False)
        monkeypatch.delenv("MOSAICO_API_KEY", raising=False)
        monkeypatch.delenv("MOSAICO_TLS", raising=False)
        monkeypatch.delenv("MOSAICO_CERT_PATH", raising=False)

        profile = MosaicoProfile.resolve(profile_name="staging")
        assert profile.host == "staging.remote"
        assert profile.port == 8080

    def test_resolve_nonexistent_named_profile_exits(self, monkeypatch, tmp_path):
        config_path = tmp_path / "config.toml"
        config_path.write_text('[dev]\nhost = "x"\ndefault = true\n')
        monkeypatch.setenv("MOSAICO_CONFIG_PATH", str(config_path))
        monkeypatch.delenv("MOSAICO_DAEMON_URL", raising=False)
        monkeypatch.delenv("MOSAICO_API_KEY", raising=False)
        monkeypatch.delenv("MOSAICO_TLS", raising=False)
        monkeypatch.delenv("MOSAICO_CERT_PATH", raising=False)

        with pytest.raises(SystemExit):
            MosaicoProfile.resolve(profile_name="ghost")

    def test_resolve_no_config_no_env_returns_empty(self, monkeypatch, tmp_path):
        config_path = tmp_path / "config.toml"
        monkeypatch.setenv("MOSAICO_CONFIG_PATH", str(config_path))
        monkeypatch.delenv("MOSAICO_DAEMON_URL", raising=False)
        monkeypatch.delenv("MOSAICO_API_KEY", raising=False)
        monkeypatch.delenv("MOSAICO_TLS", raising=False)
        monkeypatch.delenv("MOSAICO_CERT_PATH", raising=False)

        profile = MosaicoProfile.resolve()
        assert profile.host == ""

    def test_env_overrides_config(self, monkeypatch, tmp_path):
        config_path = tmp_path / "config.toml"
        config_path.write_text(
            '[dev]\nhost = "cfg.com"\nport = "6276"\n'
            'api_key = "cfgkey"\ntls = false\ndefault = true\n'
        )
        monkeypatch.setenv("MOSAICO_CONFIG_PATH", str(config_path))
        monkeypatch.setenv("MOSAICO_DAEMON_URL", "env.com:1111")
        monkeypatch.setenv("MOSAICO_API_KEY", "envkey")
        monkeypatch.setenv("MOSAICO_TLS", "yes")

        profile = MosaicoProfile.resolve()
        assert profile.host == "env.com"
        assert profile.port == 1111
        assert profile.api_key == "envkey"
        assert profile.tls is True


class TestRepr:
    def test_repr_hides_api_key(self):
        profile = MosaicoProfile(host="h", api_key="supersecret")
        r = repr(profile)
        assert "supersecret" not in r
        assert "***" in r

    def test_repr_empty_key(self):
        profile = MosaicoProfile(host="h")
        r = repr(profile)
        assert "empty" in r
