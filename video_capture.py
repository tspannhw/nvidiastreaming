import logging
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple


@dataclass
class VideoCaptureConfig:
    enabled: bool
    device_index: int
    output_dir: str
    filename_prefix: str = "orin"


def capture_frame(config: VideoCaptureConfig) -> Tuple[Optional[str], bool]:
    if not config.enabled:
        return None, False

    try:
        import cv2
    except ImportError:
        logging.warning("OpenCV not installed; video capture disabled.")
        return None, False

    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{config.filename_prefix}{uuid.uuid4()}.jpg"
    output_path = output_dir / filename

    cam = cv2.VideoCapture(config.device_index)
    try:
        if not cam.isOpened():
            logging.warning("Video capture device %s not available.", config.device_index)
            return None, False
        ok, frame = cam.read()
        if not ok:
            logging.warning("Video capture read failed on device %s.", config.device_index)
            return None, False
        cv2.imwrite(str(output_path), frame)
    finally:
        cam.release()

    return str(output_path), True
