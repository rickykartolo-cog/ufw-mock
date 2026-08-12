import argparse
import json
import sys

from ufw_mock.models.pipeline import Pipeline
from ufw_mock.models.platform import Platform
from ufw_mock.declarative.registrar import run_declarative_pipeline
from ufw_mock.runtime.platform import get_platform
from ufw_mock.runtime.pipeline_runner import PipelineRunner
from ufw_mock.validation.schema_loader import validate_pipeline_config


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="UFW mock pipeline runner.")
    parser.add_argument("--config", required=True, help="Path to the pipeline JSON config.")
    parser.add_argument(
        "--platform",
        default="local_pyspark",
        help="Runtime platform: local_pyspark, spark_declarative, or databricks.",
    )
    parser.add_argument(
        "--mode",
        choices=("imperative", "declarative"),
        default="imperative",
        help="Execution mode.",
    )
    parser.add_argument("--validate-only", action="store_true", help="Validate config without running.")
    parser.add_argument("--dry-run", action="store_true", help="Validate a declarative graph without executing it.")
    parser.add_argument("--full-refresh-all", action="store_true", help="Recompute all declarative datasets.")
    args = parser.parse_args(argv)

    with open(args.config, "r", encoding="utf-8") as f:
        raw_config = json.load(f)

    errors = validate_pipeline_config(raw_config)
    if errors:
        for error in errors:
            print(f"CONFIG ERROR: {error}", file=sys.stderr)
        return 1

    try:
        pipeline = Pipeline.model_validate(raw_config)
    except Exception as exc:
        print(f"CONFIG ERROR: {exc}", file=sys.stderr)
        return 1

    if args.validate_only:
        print("Config is valid.")
        return 0

    raw_platform = raw_config.get("platform", {})
    platform_name = (
        "spark_declarative"
        if args.mode == "declarative"
        else raw_platform.get("name", args.platform)
    )
    platform_config = Platform(name=platform_name, config=raw_platform.get("config", {}))
    platform = get_platform(platform_config)

    if args.mode == "declarative":
        if pipeline.declarative is None:
            print("CONFIG ERROR: declarative.storage is required when --mode declarative is used.", file=sys.stderr)
            return 1
        try:
            summary = run_declarative_pipeline(
                pipeline,
                platform,
                dry=args.dry_run,
                full_refresh_all=args.full_refresh_all,
            )
        finally:
            platform.shutdown()
        if args.dry_run:
            for path, dataset in summary["mapping"].items():
                print(f"PATH_MAPPING: {path} -> {dataset}")
        print(json.dumps(summary, indent=2))
        return 0

    if args.dry_run or args.full_refresh_all:
        parser.error("--dry-run and --full-refresh-all require --mode declarative")

    with PipelineRunner(pipeline, platform) as runner:
        summary = runner.run()
        print(json.dumps(summary, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
