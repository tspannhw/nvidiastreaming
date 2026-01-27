import argparse
import json
import logging
import time
from pathlib import Path
from typing import List, Optional

from jetson_metrics import collect_metrics
from ollama_client import OllamaClient, OllamaConfig
from slack_client import SlackClient, SlackConfig
from snowpipe_streaming_client import SnowpipeConfig, SnowpipeStreamingClient
from video_capture import VideoCaptureConfig, capture_frame


def _load_config(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _build_rows(
    batch_size: int,
    ollama: Optional[OllamaClient],
    video_cfg: Optional[VideoCaptureConfig],
    slack: Optional[SlackClient],
    image_prompt: Optional[str],
) -> List[dict]:
    rows = []
    sample_metrics = collect_metrics()
    summary = ollama.summarize(sample_metrics) if ollama else None
    if ollama and not summary:
        logging.warning("EDGE_AI_SUMMARY is empty; check Ollama config/model.")
    image_path, image_captured = capture_frame(video_cfg) if video_cfg else (None, False)
    if video_cfg and video_cfg.enabled and not image_captured:
        logging.warning("Video capture enabled but no image was captured.")

    image_summary = None
    if image_captured and image_path and ollama:
        image_summary = ollama.analyze_image(image_path, image_prompt)
        if not image_summary:
            logging.warning("image_ai_summary is empty; check vision model/capture.")

    if image_captured and image_path and slack:
        slack.send_image(image_path, image_summary)

    for _ in range(batch_size):
        metrics = collect_metrics()
        row = {
            "row_id": metrics["row_id"],
            "host": metrics["host"],
            "ip_address": metrics["ip_address"],
            "mac_address": metrics["mac_address"],
            "ts_utc": metrics["ts_utc"],
            "ts_epoch_ms": metrics["ts_epoch_ms"],
            "cpu_temp_c": metrics["cpu_temp_c"],
            "cpu_usage_pct": metrics["cpu_usage_pct"],
            "mem_usage_pct": metrics["mem_usage_pct"],
            "disk_usage_pct": metrics["disk_usage_pct"],
            "thermal_zones": metrics["thermal_zones"],
            "edge_ai_summary": summary,
            "image_path": image_path,
            "image_captured": image_captured,
            "image_ai_summary": image_summary,
            "payload": metrics,
        }
        rows.append(row)
    return rows


def _next_offset(current: Optional[str]) -> str:
    if current is None:
        return "1"
    try:
        return str(int(current) + 1)
    except (TypeError, ValueError):
        return str(int(time.time() * 1000))


def main() -> None:
    parser = argparse.ArgumentParser(description="Jetson Orin Snowpipe Streaming v2")
    parser.add_argument("--config", default="snowflake_config.json", help="Path to config file")
    parser.add_argument("--batch-size", type=int, default=10, help="Rows per batch")
    parser.add_argument("--interval", type=float, default=5.0, help="Seconds between batches")
    parser.add_argument("--verify-commit", action="store_true", help="Wait for commit per batch")
    parser.add_argument("--ollama-model", default=None, help="Override Ollama model name")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    config_data = _load_config(Path(args.config))
    ollama_cfg = config_data.get("ollama", {})
    video_cfg = config_data.get("video_capture", {})
    if args.ollama_model:
        ollama_cfg["model"] = args.ollama_model
    logging.debug("Config loaded: %s", args.config)

    ollama = None
    image_prompt = None
    if ollama_cfg.get("enabled"):
        logging.debug("Ollama enabled with model=%s", ollama_cfg.get("model"))
        ollama = OllamaClient(
            OllamaConfig(
                enabled=ollama_cfg.get("enabled", True),
                base_url=ollama_cfg.get("base_url", "http://localhost:11434"),
                model=ollama_cfg.get("model", "llama3.2"),
                prompt_template=ollama_cfg.get(
                    "prompt_template",
                    "Summarize the system status in one sentence: {metrics}",
                ),
                max_response_chars=ollama_cfg.get("max_response_chars", 512),
            )
        )
        image_prompt = ollama_cfg.get(
            "image_prompt_template",
            "You are analyzing a captured image provided in the request. "
            "Do not ask for a URL or description. "
            "Return a concise JSON object with keys: "
            "objects (array of strings), scene (string), anomalies (array of strings), "
            "risk_note (string).",
        )

    video_capture = None
    if video_cfg.get("enabled"):
        logging.debug("Video capture enabled on device %s", video_cfg.get("device_index", 0))
        video_capture = VideoCaptureConfig(
            enabled=video_cfg.get("enabled", True),
            device_index=int(video_cfg.get("device_index", 0)),
            output_dir=video_cfg.get("output_dir", "./captures"),
            filename_prefix=video_cfg.get("filename_prefix", "orin"),
        )

    slack_cfg = config_data.get("slack", {})
    slack_client = None
    if slack_cfg.get("enabled"):
        if not slack_cfg.get("bot_token") or not slack_cfg.get("channel"):
            raise ValueError("Slack enabled but bot_token or channel missing in config")
        slack_client = SlackClient(
            SlackConfig(
                enabled=slack_cfg.get("enabled", True),
                bot_token=slack_cfg["bot_token"],
                channel=slack_cfg["channel"],
                message_prefix=slack_cfg.get("message_prefix", "Jetson Orin Upload"),
            )
        )

    account_identifier = config_data.get("account_identifier") or config_data.get("account")
    if not account_identifier:
        raise ValueError("Missing account_identifier (or account) in config")

    pat_token = config_data.get("pat_token") or config_data.get("pat")
    auth_method = config_data.get("auth_method")
    if not auth_method:
        auth_method = "pat" if pat_token else "keypair_jwt"

    control_host = None
    if config_data.get("url"):
        control_host = config_data["url"]

    snowpipe_cfg = SnowpipeConfig(
        account_identifier=account_identifier,
        user=config_data["user"],
        role=config_data.get("role", ""),
        database=config_data["database"],
        schema=config_data["schema"],
        table=config_data["table"],
        pipe=config_data["pipe"],
        channel_name=config_data["channel_name"],
        auth_method=auth_method,
        private_key_path=config_data.get("private_key_path", ""),
        private_key_passphrase=config_data.get("private_key_passphrase") or None,
        public_key_fp=config_data.get("public_key_fp") or None,
        jwt_lifetime_seconds=int(config_data.get("jwt_lifetime_seconds", 3600)),
        pat_token=pat_token or None,
        control_host=control_host,
    )

    client = SnowpipeStreamingClient(snowpipe_cfg)
    logging.info("Connecting to Snowpipe Streaming...")
    client.connect()
    logging.info("Connected. ingest_host=%s", client.ingest_host)

    batch_number = 0
    while True:
        batch_number += 1
        rows = _build_rows(args.batch_size, ollama, video_capture, slack_client, image_prompt)
        logging.debug("Built %s rows for batch %s", len(rows), batch_number)
        offset_token = _next_offset(client.offset_token)
        logging.debug("Appending rows with offset_token=%s", offset_token)
        response = client.append_rows(rows, offset_token=offset_token)
        client.offset_token = offset_token

        print(
            f"[OK] Batch {batch_number} sent: rows={len(rows)} "
            f"offset={offset_token} next_token={response.get('next_continuation_token')}"
        )

        if args.verify_commit:
            committed = client.wait_for_commit(offset_token)
            status = "committed" if committed else "pending"
            print(f"[INFO] Batch {batch_number} commit status: {status}")

        time.sleep(args.interval)


if __name__ == "__main__":
    main()
