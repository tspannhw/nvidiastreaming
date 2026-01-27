import json
import time
import uuid
from dataclasses import dataclass
from typing import Iterable, Optional

import requests

from snowflake_jwt_auth import JwtConfig, generate_jwt


@dataclass
class SnowpipeConfig:
    account_identifier: str
    user: str
    role: str
    database: str
    schema: str
    table: str
    pipe: str
    channel_name: str
    auth_method: str
    private_key_path: str
    private_key_passphrase: Optional[str]
    public_key_fp: Optional[str]
    jwt_lifetime_seconds: int
    pat_token: Optional[str]
    control_host: Optional[str] = None


class SnowpipeStreamingClient:
    def __init__(self, config: SnowpipeConfig) -> None:
        self.config = config
        if config.control_host:
            control_host = config.control_host.replace("https://", "").replace("http://", "")
        else:
            control_host = f"{config.account_identifier}.snowflakecomputing.com"
        self.control_host = control_host
        self.ingest_host = None
        self.scoped_token = None
        self.scoped_token_type = None
        self.continuation_token = None
        self.offset_token = None

    def _headers(self, token: str, token_type: str) -> dict:
        headers = {
            "Authorization": f"Bearer {token}",
        }
        if token_type:
            headers["X-Snowflake-Authorization-Token-Type"] = token_type
        return headers

    def _request_id(self) -> str:
        return str(uuid.uuid4())

    def _jwt_token(self) -> str:
        jwt_config = JwtConfig(
            account_identifier=self.config.account_identifier,
            user=self.config.user,
            private_key_path=self.config.private_key_path,
            private_key_passphrase=self.config.private_key_passphrase,
            public_key_fp=self.config.public_key_fp or None,
            lifetime_seconds=self.config.jwt_lifetime_seconds,
        )
        return generate_jwt(jwt_config)

    def _auth_token(self) -> tuple[str, str]:
        auth_method = (self.config.auth_method or "").lower()
        if auth_method in {"pat", "programmatic_access_token", "programmatic"}:
            if not self.config.pat_token:
                raise ValueError("pat_token is required for auth_method=pat")
            return self.config.pat_token, "PROGRAMMATIC_ACCESS_TOKEN"
        return self._jwt_token(), "KEYPAIR_JWT"

    def get_ingest_host(self) -> str:
        token, token_type = self._auth_token()
        url = f"https://{self.control_host}/v2/streaming/hostname"
        response = requests.get(url, headers=self._headers(token, token_type), timeout=30)
        response.raise_for_status()
        host = None
        try:
            payload = response.json()
            host = payload.get("hostname")
        except requests.exceptions.JSONDecodeError:
            host = (response.text or "").strip()

        if not host:
            snippet = (response.text or "").strip()[:500]
            raise RuntimeError(
                "Missing hostname in response. "
                f"status={response.status_code} content_type={response.headers.get('Content-Type')} "
                f"body={snippet}"
            )
        if "_" in host:
            host = host.replace("_", "-")
        self.ingest_host = host
        return host

    def exchange_scoped_token(self) -> str:
        if self.config.auth_method.lower() == "pat":
            self.scoped_token = self.config.pat_token
            self.scoped_token_type = "PROGRAMMATIC_ACCESS_TOKEN"
            return self.scoped_token

        token, token_type = self._auth_token()
        url = f"https://{self.control_host}/oauth/token"
        payload = {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "scope": self.ingest_host,
        }
        headers = self._headers(token, token_type)
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        response = requests.post(url, data=payload, headers=headers, timeout=30)
        response.raise_for_status()
        self.scoped_token = response.json()["token"]
        self.scoped_token_type = "OAUTH"
        return self.scoped_token

    def open_channel(self, offset_token: Optional[str] = None) -> dict:
        if not self.ingest_host:
            raise RuntimeError("ingest_host not initialized")
        url = (
            f"https://{self.ingest_host}/v2/streaming/databases/{self.config.database}"
            f"/schemas/{self.config.schema}/pipes/{self.config.pipe}/channels/{self.config.channel_name}"
        )
        payload = {}
        if offset_token is not None:
            payload["offset_token"] = offset_token
        response = requests.put(
            url,
            headers=self._headers(self.scoped_token, self.scoped_token_type),
            json=payload,
            params={"requestId": self._request_id()},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        self.continuation_token = data["next_continuation_token"]
        self.offset_token = data["channel_status"].get("last_committed_offset_token")
        return data

    def append_rows(self, rows: Iterable[dict], offset_token: Optional[str] = None) -> dict:
        if not self.ingest_host or not self.continuation_token:
            raise RuntimeError("channel not opened")

        ndjson = "".join(json.dumps(row) + "\n" for row in rows)
        params = {"continuationToken": self.continuation_token}
        if offset_token is not None:
            params["offsetToken"] = offset_token

        url = (
            f"https://{self.ingest_host}/v2/streaming/data/databases/{self.config.database}"
            f"/schemas/{self.config.schema}/pipes/{self.config.pipe}/channels/{self.config.channel_name}/rows"
        )
        headers = self._headers(self.scoped_token, self.scoped_token_type)
        headers["Content-Type"] = "application/x-ndjson"
        response = requests.post(url, headers=headers, params=params, data=ndjson.encode("utf-8"), timeout=30)
        response.raise_for_status()
        data = response.json()
        self.continuation_token = data["next_continuation_token"]
        return data

    def get_channel_status(self) -> dict:
        url = (
            f"https://{self.ingest_host}/v2/streaming/databases/{self.config.database}"
            f"/schemas/{self.config.schema}/pipes/{self.config.pipe}:bulk-channel-status"
        )
        payload = {"channel_names": [self.config.channel_name]}
        response = requests.post(
            url,
            headers=self._headers(self.scoped_token, self.scoped_token_type),
            json=payload,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def drop_channel(self) -> None:
        url = (
            f"https://{self.ingest_host}/v2/streaming/databases/{self.config.database}"
            f"/schemas/{self.config.schema}/pipes/{self.config.pipe}/channels/{self.config.channel_name}"
        )
        response = requests.delete(
            url,
            headers=self._headers(self.scoped_token, self.scoped_token_type),
            params={"requestId": self._request_id()},
            timeout=30,
        )
        response.raise_for_status()

    def connect(self) -> None:
        self.get_ingest_host()
        self.exchange_scoped_token()
        self.open_channel()

    def wait_for_commit(self, expected_offset: Optional[str], timeout_seconds: int = 60) -> bool:
        if expected_offset is None:
            return True

        start = time.time()
        while time.time() - start < timeout_seconds:
            status = self.get_channel_status()
            channel = status.get("channel_statuses", {}).get(self.config.channel_name, {})
            committed = channel.get("last_committed_offset_token")
            if committed is not None and str(committed) >= str(expected_offset):
                return True
            time.sleep(1)
        return False
