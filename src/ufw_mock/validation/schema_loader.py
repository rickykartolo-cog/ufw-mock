import json
import pathlib

from jsonschema import Draft202012Validator, ValidationError


_SCHEMA_PATH = pathlib.Path(__file__).parent.parent / "config" / "pipeline_schema.json"


def load_pipeline_schema() -> dict:
    with _SCHEMA_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def validate_pipeline_config(config: dict) -> list[str]:
    """Validate a raw pipeline config dict against the JSON schema.

    Returns a list of human-readable error messages. An empty list means the
    config is valid.
    """
    schema = load_pipeline_schema()
    validator = Draft202012Validator(schema)
    errors = [str(e) for e in validator.iter_errors(config)]
    return errors
