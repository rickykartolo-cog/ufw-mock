from ufw_mock.validation.schema_loader import validate_pipeline_config


def test_valid_config():
    config = {
        "name": "test",
        "tasks": [
            {
                "id": "t1",
                "type": "INGEST",
                "source": {"edge_node": "inbound", "path": "raw"},
                "target": {"edge_node": "core", "path": "curated"},
            }
        ],
    }
    errors = validate_pipeline_config(config)
    assert errors == []


def test_missing_required_field():
    config = {
        "tasks": [
            {
                "id": "t1",
                "type": "INGEST",
                "source": {"edge_node": "inbound", "path": "raw"},
                "target": {"edge_node": "core", "path": "curated"},
            }
        ],
    }
    errors = validate_pipeline_config(config)
    assert any("name" in error for error in errors)


def test_invalid_task_type():
    config = {
        "name": "test",
        "tasks": [
            {
                "id": "t1",
                "type": "WRONG",
                "source": {"edge_node": "inbound", "path": "raw"},
                "target": {"edge_node": "core", "path": "curated"},
            }
        ],
    }
    errors = validate_pipeline_config(config)
    assert errors
