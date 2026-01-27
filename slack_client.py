import logging
from dataclasses import dataclass
from typing import Optional

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


@dataclass
class SlackConfig:
    enabled: bool
    bot_token: str
    channel: str
    message_prefix: str = "Jetson Orin Upload"


class SlackClient:
    def __init__(self, config: SlackConfig) -> None:
        self.config = config
        self.client = WebClient(token=config.bot_token)

    def send_image(self, image_path: str, caption: Optional[str]) -> None:
        if not self.config.enabled:
            return

        text = self.config.message_prefix
        if caption:
            text = f"{text}: {caption}"

        try:
            self.client.chat_postMessage(channel=self.config.channel, text=text)
            self.client.files_upload_v2(
                channel=self.config.channel,
                file=image_path,
                title=caption or "Jetson Orin Capture",
                initial_comment=text,
            )
        except SlackApiError as exc:
            logging.warning("Slack upload failed: %s", exc.response.get("error"))
