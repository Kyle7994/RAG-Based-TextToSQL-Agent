import pytest

def test_query_debug_not_initialized(client, monkeypatch):
    monkeypatch.setattr("app.api.routes.get_current_schema_version", lambda: None)

    resp = client.post("/query/debug", json={"question": "most selled products"})
    data = resp.json()

    assert resp.status_code == 200
    assert data["cache_status"] == "not_initialized"
    assert data["generated_sql"] is None
    assert data["answerable"] is False
    assert "sync-schema" in data["error"]

def test_query_debug_cache_hit(client, monkeypatch):
    monkeypatch.setattr("app.api.routes.get_current_schema_version", lambda: "v1")
    monkeypatch.setattr(
        "app.api.routes.get_cached_response",
        lambda question, schema_version: {
            "query_plan": "plan",
            "sql": "SELECT 1",
            "uncertainty_note": None,
            "answerable": True,
            "status": "cache_hit",
            "cache_level": "redis",
            "error": None,
        },
    )
    async def fake_build_generation_context(question):
        assert question == "test"
        return ("schema_context", "examples_context")

    def fake_validate_guard_and_explain(question, sql, schema_context):
        assert question == "test"
        assert sql == "SELECT 1"
        assert schema_context == "schema_context"
        return ("SELECT 1", True, None, [], True, None)

    monkeypatch.setattr("app.api.routes.build_generation_context", fake_build_generation_context)
    monkeypatch.setattr("app.api.routes._validate_guard_and_explain", fake_validate_guard_and_explain)

    resp = client.post("/query/debug", json={"question": "test"})
    data = resp.json()

    assert resp.status_code == 200
    assert data["is_cached"] is True
    assert data["generated_sql"] == "SELECT 1"
    assert data["cache_status"] == "cache_hit"

@pytest.mark.asyncio
async def test_query_debug_generate(client, monkeypatch):
    monkeypatch.setattr("app.api.routes.get_current_schema_version", lambda: "v1")
    monkeypatch.setattr("app.api.routes.get_cached_response", lambda *args, **kwargs: None)

    async def fake_build_generation_context(question):
        return ("schema_context", "examples_context")

    async def fake_generate_sql_from_question(question, schema_context, examples_context, debug):
        assert debug is True
        return ("query plan", "SELECT * FROM orders", None, True)

    monkeypatch.setattr("app.api.routes.build_generation_context", fake_build_generation_context)
    monkeypatch.setattr("app.api.routes.generate_sql_from_question", fake_generate_sql_from_question)

    resp = client.post("/query/debug", json={"question": "查订单"})
    data = resp.json()

    assert resp.status_code == 200
    assert data["is_cached"] is False
    assert data["generated_sql"] == "SELECT * FROM orders"
    assert data["query_plan"] == "query plan"
    assert data["schema_version"] == "v1"