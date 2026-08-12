import argparse
import json
import sys

from ufw_mock.models.pipeline import Pipeline
from ufw_mock.models.platform import Platform
from ufw_mock.runtime.platform import get_platform
from ufw_mock.runtime.pipeline_runner import PipelineRunner
from ufw_mock.validation.schema_loader import validate_pipeline_config


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="UFW mock pipeline runner.")
    parser.add_argument("--config", required=True, help="Path to the pipeline JSON config.")
    parser.add_argument(
        "--platform",
        default="local_pyspark",
        help="Runtime platform: local_pyspark or databricks.",
    )
    parser.add_argument("--validate-only", action="store_true", help="Validate config without running.")
    args = parser.parse_args(argv)

    with open(args.config, "r", encoding="utf-8") as f:
        raw_config = json.load(f)

    errors = validate_pipeline_config(raw_config)
    if errors:
        for error in errors:
            print(f"CONFIG ERROR: {error}", file=sys.stderr)
        return 1

    if args.validate_only:
        print("Config is valid.")
        return 0

    pipeline = Pipeline.model_validate(raw_config)
    raw_platform = raw_config.get("platform", {})
    platform_name = raw_platform.get("name", args.platform)
    platform_config = Platform(name=platform_name, config=raw_platform.get("config", {}))
    platform = get_platform(platform_config)

    with PipelineRunner(pipeline, platform) as runner:
        summary = runner.run()
        print(json.dumps(summary, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
