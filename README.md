Jetson Orin Snowpipe Streaming v2
=================================

High-speed Snowpipe Streaming v2 (REST API) client for NVIDIA Jetson AGX Orin
with optional Ollama-based edge AI enrichment. This project follows the same
patterns as the Raspberry Pi and Kafka examples but targets Jetson hardware.

Key features
------------
- Snowpipe Streaming v2 REST API with scoped token flow.
- Key-pair JWT or Programmatic Access Token authentication.
- Jetson-friendly system metrics collection.
- Optional Ollama enrichment per batch.

Quick start
-----------
1) Create a Python virtual environment and install dependencies:

   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt

2) Create Snowflake objects:

   - Run `setup_snowflake.sql` in Snowsight.
   - The default streaming pipe for the table is `<TABLE>-STREAMING`.

3) Create your config:

   cp snowflake_config.json.template snowflake_config.json
   # Edit the file and fill in account/user/database/schema/table/pipe.
   # PAT format matches the RPIWeatherStreaming example:
   #   account, url, pat

4) (Optional) Start Ollama and pull a model:

   ollama serve
   ollama pull llama3.2-vision

5) Run the streamer:

   python main.py --config snowflake_config.json --batch-size 25 --interval 5.0
   python main.py --ollama-model llama3.2-vision

Debug mode
----------
Add `--debug` to print verbose progress and configuration hints:

  python main.py --debug --batch-size 10 --interval 5.0

Video capture
-------------
To capture a frame per batch (similar to `orin.py`), enable the video section
in `snowflake_config.json`:

  "video_capture": {
    "enabled": true,
    "device_index": 0,
    "output_dir": "./captures",
    "filename_prefix": "orin"
  }

Slack upload + Ollama image analysis
-----------------------------------
To send captured images to Slack and get a short Ollama caption:

  "slack": {
    "enabled": true,
    "bot_token": "xoxb-...",
    "channel": "C0123456789",
    "message_prefix": "Jetson Orin Upload"
  }

When enabled, the captured image is sent to Ollama for a one-sentence summary
and then uploaded to Slack with that summary.

Notes on Snowpipe Streaming v2
------------------------------
This project uses the Snowpipe Streaming REST API flow:

- Get ingest host
- Exchange scoped token
- Open channel
- Append NDJSON rows
- Check channel status

See Snowflake documentation for endpoint details and the JWT flow:
https://docs.snowflake.com/en/user-guide/snowpipe-streaming-high-performance-rest-api
https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-rest-tutorial
https://docs.snowflake.com/en/developer-guide/snowflake-rest-api/authentication

Project structure
-----------------
- main.py: Entry point and CLI.
- snowpipe_streaming_client.py: REST API client for Snowpipe Streaming v2.
- snowflake_jwt_auth.py: JWT creation for key-pair authentication.
- jetson_metrics.py: System metrics collection on Jetson.
- ollama_client.py: Optional edge AI enrichment.
- setup_snowflake.sql: Database and table setup.
- snowflake_config.json.template: Configuration template.

Configuration tips
------------------
- Use `account_identifier` format `ORG-ACCOUNT` in uppercase.
- If your account hostname contains underscores, replace them with dashes
  in the ingest host (per Snowflake docs).
- Keep batches below the 16 MB payload limit (4 MB for NDJSON rows).
