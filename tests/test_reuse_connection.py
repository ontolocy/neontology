"""init_neontology(reuse_connection=True), on the NetworkX engine because it needs no database."""

import pytest

from neontology.graphconnection import GraphConnection, init_neontology

pytest.importorskip("grandcypher", reason="needs the [grand] extra")

from neontology.graphengines import NetworkxConfig  # noqa: E402


class NamedNetworkxConfig(NetworkxConfig):
    """Configs which differ from one another, for an engine needing no database."""

    name: str = "a"


@pytest.fixture
def connection_of_its_own():
    """Start with no connection, then put the suite's back, closing any made meanwhile."""
    session_instance = GraphConnection._instance

    GraphConnection._instance = None

    try:
        yield

    finally:
        established = GraphConnection._instance

        GraphConnection._instance = session_instance

        if established is not None:
            established.engine.close_connection()


@pytest.mark.usefixtures("connection_of_its_own")
class TestReuseConnection:
    """reuse_connection keeps the live connection only when it was made with an equal config."""

    def test_connects_when_nothing_is_live(self):
        init_neontology(NamedNetworkxConfig(), reuse_connection=True)

        assert GraphConnection().engine.verify_connection() is True

    def test_keeps_a_connection_made_with_an_equal_config(self):
        init_neontology(NamedNetworkxConfig())
        engine = GraphConnection().engine

        init_neontology(NamedNetworkxConfig(), reuse_connection=True)

        assert GraphConnection().engine is engine

    def test_reconnects_with_a_different_config(self):
        init_neontology(NamedNetworkxConfig(name="a"))
        engine = GraphConnection().engine

        init_neontology(NamedNetworkxConfig(name="b"), reuse_connection=True)

        assert GraphConnection().engine is not engine

    def test_reconnects_when_the_config_has_changed_since_connecting(self):
        """The config is recorded as it was when connecting, not as the caller's object is now."""
        config = NamedNetworkxConfig(name="a")
        init_neontology(config)
        engine = GraphConnection().engine

        config.name = "b"
        init_neontology(config, reuse_connection=True)

        assert GraphConnection().engine is not engine

    def test_compares_a_config_read_from_the_environment(self, monkeypatch):
        monkeypatch.setenv("NEONTOLOGY_ENGINE", "NETWORKX")

        init_neontology()
        engine = GraphConnection().engine

        init_neontology(reuse_connection=True)

        assert GraphConnection().engine is engine

    def test_logs_the_config_it_connected_with(self, caplog):
        with caplog.at_level("INFO", logger="neontology.graphconnection"):
            init_neontology(NamedNetworkxConfig(name="logged"))

        assert "NamedNetworkxConfig(read_only=False, name='logged')" in caplog.text

    def test_reconnects_by_default(self):
        init_neontology(NamedNetworkxConfig())
        engine = GraphConnection().engine

        init_neontology(NamedNetworkxConfig())

        assert GraphConnection().engine is not engine
