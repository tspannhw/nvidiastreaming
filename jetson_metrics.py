import logging
import socket
import time
import uuid
from pathlib import Path
from typing import Dict, Optional

import psutil


def _read_thermal_zones() -> Dict[str, float]:
    zones = {}
    base = Path("/sys/devices/virtual/thermal")
    if not base.exists():
        return zones

    for zone in base.glob("thermal_zone*"):
        type_file = zone / "type"
        temp_file = zone / "temp"
        if not type_file.exists() or not temp_file.exists():
            continue
        try:
            zone_type = _safe_read_text(type_file)
            temp_raw = _safe_read_text(temp_file)
            if not zone_type or not temp_raw:
                continue
            temp_c = float(temp_raw) / 1000.0
            zones[zone_type] = temp_c
        except (OSError, ValueError, TypeError) as exc:
            logging.debug("Failed reading thermal zone %s: %s", zone, exc)
            continue
    return zones


def _cpu_temp_from_zones(zones: Dict[str, float]) -> Optional[float]:
    if not zones:
        return None
    for key in ("CPU-therm", "cpu-thermal", "CPU"):
        if key in zones:
            return zones[key]
    return max(zones.values())


def collect_metrics() -> Dict[str, object]:
    host = socket.gethostname()
    ip_address, mac_address = _get_primary_network_info()
    now = time.time()
    zones = _read_thermal_zones()

    return {
        "row_id": str(uuid.uuid4()),
        "host": host,
        "ip_address": ip_address,
        "mac_address": mac_address,
        "ts_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
        "ts_epoch_ms": int(now * 1000),
        "cpu_temp_c": _cpu_temp_from_zones(zones),
        "cpu_usage_pct": psutil.cpu_percent(interval=None),
        "mem_usage_pct": psutil.virtual_memory().percent,
        "disk_usage_pct": psutil.disk_usage("/").percent,
        "thermal_zones": zones,
    }


def _safe_read_text(path: Path) -> str:
    try:
        return path.read_text().strip()
    except Exception:
        try:
            data = path.read_bytes()
            return data.decode("utf-8", errors="ignore").strip()
        except Exception:
            return ""


def _get_primary_network_info() -> tuple[str, str]:
    stats = psutil.net_if_stats()
    addrs_map = psutil.net_if_addrs()

    for iface, iface_stats in stats.items():
        if not iface_stats.isup or iface.startswith("lo"):
            continue
        ip = None
        mac = None
        for addr in addrs_map.get(iface, []):
            if addr.family == socket.AF_INET:
                if addr.address and not addr.address.startswith("127.") and not addr.address.startswith("169.254."):
                    ip = addr.address
            elif addr.family == psutil.AF_LINK:
                if addr.address and addr.address != "00:00:00:00:00:00":
                    mac = addr.address
        if ip or mac:
            return ip or "unknown", mac or "unknown"

    try:
        fallback_ip = socket.gethostbyname(socket.gethostname())
    except OSError:
        fallback_ip = "unknown"

    node = uuid.getnode()
    if (node >> 40) % 2:
        fallback_mac = "unknown"
    else:
        fallback_mac = ":".join(f"{(node >> ele) & 0xff:02x}" for ele in range(40, -1, -8))
    return fallback_ip, fallback_mac
