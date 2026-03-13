"""
telematics_simulator_with_osrm.py

- Loads OSRM Seattle->Tacoma route
- Simulates telemetry
- Reads next stop + ETA window from Postgres (delivery_plans + delivery_stops)
- Window-driven deviation with "roll-forward alignment" (方案 B):
    DB timestamps may be historical; we roll the window forward by whole days
    until it's not far in the past.
- Severity:
    lateness < 20min  -> LOW
    lateness < 60min  -> MEDIUM
    lateness >= 60min -> HIGH
"""

from __future__ import annotations

import atexit
import math
import os
import random
import signal
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

import psycopg2
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================
# Configuration
# =========================

API_URL = "https://15eqsal673.execute-api.us-west-2.amazonaws.com/gps"
API_URL_Reroute = "https://15eqsal673.execute-api.us-west-2.amazonaws.com/reroute"
VEHICLE_ID = "TRUCK_001"
OSRM_ROUTER = "http://router.project-osrm.org"

START_COORD_LONLAT = (-122.3321, 47.6062)
END_COORD_LONLAT = (-122.4443, 47.2529)

STEP_INTERVAL_SEC = 1  # 每1秒更新一次，便于观察快速变化

# "Deviation window" used as demo anomaly injector
# Trigger deviation earlier for testing: fire at 8s runtime
DEVIATE_AFTER_SEC = 18  # 8秒后开始偏离 (调整为触发 medium alert)
DEVIATE_DURATION_SEC = 20  # 偏离持续40秒
DEVIATE_EAST_METERS = 180  # 偏离180米

# Runtime switch to disable deviation for coarse navigation (快速观察用)
DISABLE_DEVIATION = False

# Disable incidents and time-compress trip for demo
DISABLE_INCIDENTS = False
TRIP_DURATION_SEC = 300  # seconds (5 minutes)

# Window-driven deviation control
LATE_GRACE_MIN = 3
OFFROUTE_TRIGGER_SEC = 10  # 偏离10秒后触发 (40+10=50秒总触发时间)
EVENT_THROTTLE_SEC = 30

# Speed slowdown during deviation window (tuned to stable MEDIUM)
SLOWDOWN_FACTOR = 0.6     # 降到60%速度
SLOWDOWN_MIN_KMH = 45.0    # 最低速度45 km/h

# Stop arrival radius
STOP_ARRIVAL_RADIUS_M = 120

# ETA impact scaling (demo)
ETA_IMPACT_PCT_MEDIUM = 0.25  # 增加 ETA 影响到25%

# DB stop refresh
STOP_REFRESH_SEC = int(os.environ.get("STOP_REFRESH_SEC", "10"))

# Networking
CONNECT_TIMEOUT_SEC = 3
READ_TIMEOUT_SEC = 15

RETRY_TOTAL = 3
RETRY_BACKOFF_FACTOR = 0.5
RETRY_STATUS_FORCELIST = (429, 500, 502, 503, 504)

SEND_BACKOFF_MAX_MULT = 8
FAILS_TO_INCREASE_BACKOFF = 2
SUCCESS_TO_RESET_BACKOFF = 2

# DB timestamps are TIMESTAMP (no tz) in your schema.
# For least surprise: use naive local time for "now" in comparisons.
DEMO_TZ_MODE = os.environ.get("DEMO_TZ_MODE", "LOCAL").upper()  # LOCAL or UTC

# =========================
# DB Defaults (demo)
# =========================
DB_DEFAULTS = {
    "DB_HOST": "demo-gps-db-public.cneuisio87p1.us-west-2.rds.amazonaws.com",
    "DB_PORT": "5432",
    "DB_NAME": "gps_demo",
    "DB_USER": "PostgreSQL",
    "DB_PASSWORD": "1234567890",
    # optional:
    # "PLAN_ID": "PLAN_DEMO_20260127",
}


# =========================
# Utilities
# =========================

def now_for_db_compare() -> datetime:
    if DEMO_TZ_MODE == "UTC":
        # Still naive (no tzinfo) to match TIMESTAMP, but in UTC clock
        return datetime.utcnow()
    return datetime.now()


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    return 2 * r * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def offset_meters(lat: float, lon: float, north_m: float = 0.0, east_m: float = 0.0) -> Tuple[float, float]:
    dlat = north_m / 111320.0
    dlon = east_m / (111320.0 * math.cos(math.radians(lat)))
    return lat + dlat, lon + dlon


def build_retry_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=RETRY_TOTAL,
        connect=RETRY_TOTAL,
        read=RETRY_TOTAL,
        backoff_factor=RETRY_BACKOFF_FACTOR,
        status_forcelist=RETRY_STATUS_FORCELIST,
        allowed_methods=frozenset(["POST"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_real_route(
    osrm_url: str,
    start_lonlat: Tuple[float, float],
    end_lonlat: Tuple[float, float],
) -> Tuple[Optional[List[Tuple[float, float]]], int]:
    url = (
        f"{osrm_url}/route/v1/driving/"
        f"{start_lonlat[0]},{start_lonlat[1]};{end_lonlat[0]},{end_lonlat[1]}"
        f"?overview=full&geometries=geojson&steps=false"
    )
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            return None, 0
        data = resp.json()
        if not data.get("routes"):
            return None, 0
        route0 = data["routes"][0]
        coords = [(pt[1], pt[0]) for pt in route0["geometry"]["coordinates"]]
        duration_sec = int(route0.get("duration", 0))
        # Debug: print first/last point for quick verification
        if coords:
            print(f"✅ OSRM route loaded: {len(coords)} points, baseline duration={duration_sec}s")
            print("OSRM points:", len(coords), "first:", coords[0], "last:", coords[-1])
        else:
            print(f"✅ OSRM route loaded: 0 points")
        return coords, duration_sec
    except Exception as exc:
        print(f"⚠️ OSRM fetch failed: {exc}. Falling back to straight-line route.")
        return None, 0


def severity_from_lateness_min(lateness_min: int) -> str:
    if lateness_min < 5:  # 5分钟以内为LOW
        return "LOW"
    if lateness_min < 1440:  # 5-1440分钟为MEDIUM
        return "MEDIUM"
    return "HIGH"  # 超过24小时才是HIGH


def compute_lateness_min(predicted_arrival: datetime, window_end: datetime) -> int:
    lateness_sec = (predicted_arrival - window_end).total_seconds()
    if lateness_sec <= 0:
        return 0
    return int((lateness_sec + 59) // 60)


def align_window_to_now(
    window_start: datetime,
    window_end: datetime,
    now: datetime,
    *,
    past_grace_sec: int = 2 * 3600,
    max_shift_days: int = 7,
) -> Tuple[datetime, datetime, int]:
    """
    直接将数据库中的时间窗口日期部分改成今天，保持时间部分不变。
    返回 (aligned_start, aligned_end, shift_days)
    """
    # 获取今天的日期
    today = now.date()
    
    # 将窗口的日期改成今天，保持原来的时间部分
    aligned_start = datetime.combine(today, window_start.time())
    aligned_end = datetime.combine(today, window_end.time())
    
    # 计算移动了多少天
    shift_days = (today - window_start.date()).days
    
    return aligned_start, aligned_end, shift_days


# 全局变量：备份原始stop状态
ORIGINAL_STOPS_STATUS = {}
ORIGINAL_STOPS_SEQUENCE = {}  # 新增：备份 current_sequence
PLAN_ID_GLOBAL = None

# =========================
# Backup & Restore Functions
# =========================

def backup_stops_status(conn, plan_id: str, vehicle_id: str) -> None:
    """备份所有stops的原始status和current_sequence"""
    global ORIGINAL_STOPS_STATUS, ORIGINAL_STOPS_SEQUENCE
    try:
        sql = """
        SELECT stop_id, status, current_sequence
        FROM delivery_stops
        WHERE plan_id = %s AND vehicle_id = %s
        ORDER BY stop_id;
        """
        with conn.cursor() as cur:
            cur.execute(sql, (plan_id, vehicle_id))
            rows = cur.fetchall()
        
        ORIGINAL_STOPS_STATUS = {row[0]: row[1] for row in rows}
        ORIGINAL_STOPS_SEQUENCE = {row[0]: row[2] for row in rows}
        print(f"✅ Backed up {len(ORIGINAL_STOPS_STATUS)} stops (status + sequence)")
    except Exception as e:
        print(f"⚠️ Backup failed: {e}")

def restore_stops_status(plan_id: str, vehicle_id: str) -> None:
    """恢复所有stops的原始status和current_sequence"""
    global ORIGINAL_STOPS_STATUS, ORIGINAL_STOPS_SEQUENCE
    if not ORIGINAL_STOPS_STATUS and not ORIGINAL_STOPS_SEQUENCE:
        return
    
    try:
        conn = db_conn()
        try:
            with conn.cursor() as cur:
                # Step 1: Set all to negative temporary values to avoid unique constraint
                for stop_id in ORIGINAL_STOPS_STATUS.keys():
                    original_sequence = ORIGINAL_STOPS_SEQUENCE.get(stop_id)
                    if original_sequence is not None:
                        cur.execute(
                            """
                            UPDATE delivery_stops
                            SET current_sequence = %s
                            WHERE plan_id = %s AND vehicle_id = %s AND stop_id = %s;
                            """,
                            (-original_sequence, plan_id, vehicle_id, stop_id),
                        )

                # Step 2: Restore status and set positive sequence values
                for stop_id in ORIGINAL_STOPS_STATUS.keys():
                    original_status = ORIGINAL_STOPS_STATUS.get(stop_id)
                    original_sequence = ORIGINAL_STOPS_SEQUENCE.get(stop_id)

                    if original_status and original_sequence is not None:
                        cur.execute(
                            """
                            UPDATE delivery_stops
                            SET status = %s, current_sequence = %s
                            WHERE plan_id = %s AND vehicle_id = %s AND stop_id = %s;
                            """,
                            (original_status, original_sequence, plan_id, vehicle_id, stop_id),
                        )
            conn.commit()
            print(f"✅ Restored {len(ORIGINAL_STOPS_STATUS)} stops to original status and sequence")
        finally:
            conn.close()
    except Exception as e:
        print(f"⚠️ Restore failed: {e}")

def cleanup_handler(signum, frame):
    """Ctrl+C信号处理器"""
    print("\n⏹️  收到退出信号，正在恢复数据库...")
    if PLAN_ID_GLOBAL and VEHICLE_ID:
        restore_stops_status(PLAN_ID_GLOBAL, VEHICLE_ID)
    sys.exit(0)


# =========================
# DB access
# =========================

def _get_cfg(key: str, default: Optional[str] = None) -> str:
    # env 优先，其次用代码默认值
    v = os.environ.get(key)
    if v is not None and str(v).strip() != "":
        return v
    if key in DB_DEFAULTS:
        return DB_DEFAULTS[key]
    if default is not None:
        return default
    raise RuntimeError(f"Missing config for {key} (set env or DB_DEFAULTS).")

def db_conn():
    host = _get_cfg("DB_HOST")
    port = int(_get_cfg("DB_PORT", "5432"))
    dbname = _get_cfg("DB_NAME")
    user = _get_cfg("DB_USER")
    password = _get_cfg("DB_PASSWORD")

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        connect_timeout=5,
    )


def fetch_active_plan_id(conn, vehicle_id: str) -> Optional[str]:
    plan_id = os.environ.get("PLAN_ID") or DB_DEFAULTS.get("PLAN_ID")
    if plan_id:
        return plan_id

    sql = """
    SELECT plan_id
    FROM delivery_plans
    WHERE vehicle_id = %s AND status = 'ACTIVE'
    ORDER BY created_at DESC
    LIMIT 1;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (vehicle_id,))
        row = cur.fetchone()
    return row[0] if row else None


def fetch_stops_for_plan(conn, plan_id: str, vehicle_id: str) -> List[Dict[str, Any]]:
    sql = """
    SELECT
        stop_id,
        vehicle_id,
        ST_Y(location::geometry) AS lat,
        ST_X(location::geometry) AS lon,
        original_sequence,
        current_sequence,
        planned_time_start,
        planned_time_end,
        current_time_start,
        current_time_end,
        status
    FROM delivery_stops
    WHERE plan_id = %s AND vehicle_id = %s
    ORDER BY current_sequence ASC;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (plan_id, vehicle_id))
        rows = cur.fetchall()

    stops: List[Dict[str, Any]] = []
    for r in rows:
        stops.append(
            {
                "stop_id": r[0],
                "vehicle_id": r[1],
                "lat": float(r[2]) if r[2] is not None else None,
                "lon": float(r[3]) if r[3] is not None else None,
                "original_sequence": int(r[4]) if r[4] is not None else None,
                "current_sequence": int(r[5]) if r[5] is not None else None,
                "planned_time_start": r[6],
                "planned_time_end": r[7],
                "current_time_start": r[8],
                "current_time_end": r[9],
                "status": r[10],
            }
        )
    return stops

def pick_target_stop(stops: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Target stop selection for deviation:
    - If there is an IN_PROGRESS stop, target the smallest current_sequence PENDING stop
      (i.e., the "next" stop after current work).
    - Else target the smallest current_sequence PENDING stop.
    """
    pending = [s for s in stops if (s.get("status") or "").upper() == "PENDING"]
    if not pending:
        return None

    # If any stop is IN_PROGRESS, still target the next PENDING stop (not the same IN_PROGRESS stop)
    in_prog_exists = any((s.get("status") or "").upper() == "IN_PROGRESS" for s in stops)
    if in_prog_exists:
        return sorted(pending, key=lambda x: x.get("current_sequence") or 999999)[0]

    return sorted(pending, key=lambda x: x.get("current_sequence") or 999999)[0]

def advance_stop_status_if_arrived(conn, plan_id: str, vehicle_id: str, arrived_stop_id: str) -> None:
    """
    When arrived at a stop:
    - mark that stop DONE (if it was IN_PROGRESS or PENDING)
    - mark the next PENDING stop as IN_PROGRESS (if any)
    """
    with conn.cursor() as cur:
        # 1) mark arrived stop DONE
        cur.execute(
            """
            UPDATE delivery_stops
            SET status = 'DONE'
            WHERE plan_id=%s AND vehicle_id=%s AND stop_id=%s
              AND status IN ('IN_PROGRESS','PENDING');
            """,
            (plan_id, vehicle_id, arrived_stop_id),
        )

        # 2) promote next pending to IN_PROGRESS (lowest current_sequence)
        cur.execute(
            """
            UPDATE delivery_stops
            SET status = 'IN_PROGRESS'
            WHERE stop_id = (
                SELECT stop_id FROM delivery_stops
                WHERE plan_id=%s AND vehicle_id=%s AND status='PENDING'
                ORDER BY current_sequence ASC
                LIMIT 1
            );
            """,
            (plan_id, vehicle_id),
        )

def apply_stop_order_to_db(conn, plan_id, vehicle_id, stop_order):
    """
    Rewrite current_sequence according to stop_order.
    Stops not in stop_order keep relative order and are appended.
    """
    with conn.cursor() as cur:
        # 1. Fetch remaining stops ordered by current_sequence
        cur.execute("""
            SELECT stop_id, current_sequence
            FROM delivery_stops
            WHERE plan_id = %s
              AND vehicle_id = %s
              AND status != 'DONE'
            ORDER BY current_sequence ASC
        """, (plan_id, vehicle_id))

        rows = cur.fetchall()
        remain_ids = [r[0] for r in rows]

        # 2. Build new order
        new_order = []
        for sid in stop_order:
            if sid in remain_ids:
                new_order.append(sid)

        for sid in remain_ids:
            if sid not in new_order:
                new_order.append(sid)

        # 3. First set all to negative values to avoid unique constraint conflict
        for idx, sid in enumerate(new_order, start=1):
            cur.execute("""
                UPDATE delivery_stops
                SET current_sequence = %s
                WHERE plan_id = %s
                  AND vehicle_id = %s
                  AND stop_id = %s
            """, (-(idx), plan_id, vehicle_id, sid))

        # 4. Then set to positive final values
        for idx, sid in enumerate(new_order, start=1):
            cur.execute("""
                UPDATE delivery_stops
                SET current_sequence = %s
                WHERE plan_id = %s
                  AND vehicle_id = %s
                  AND stop_id = %s
            """, (idx, plan_id, vehicle_id, sid))
    conn.commit()

import json

def record_stop_resequence_result(conn, plan_id: str, incident_id: str, old_sequence, new_sequence):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO stop_resequence_results (result_id, plan_id, incident_id, old_sequence, new_sequence)
            VALUES (gen_random_uuid(), %s, %s, %s::jsonb, %s::jsonb);
            """,
            (plan_id, incident_id, json.dumps(old_sequence), json.dumps(new_sequence)),
        )


# =========================
# Simulator
# =========================

@dataclass
class TelemetryPayload:
    vehicle_id: str
    timestamp: float
    latitude: float
    longitude: float
    location: dict
    speed: float
    heading: Optional[float]
    CAN_speed: float
    DTC_codes: List[str]
    engine_status: str
    task_state: str
    planned_latitude: float
    planned_longitude: float
    deviation_m: float
    route_baseline_duration_sec: int
    distance_ratio: float
    route_total_m: float
    traveled_m: float
    eta_sec: Optional[int]
    prev_eta_sec: Optional[int]
    eta_delta_sec: Optional[int]

    # incident fields expected by your lambda
    incident_id: Optional[str]
    event_type: Optional[str]
    event_severity: Optional[str]
    eta_impact_min: Optional[int]
    requires_reorder: Optional[bool]

    # extra debug fields forwarded downstream
    next_stop_id: Optional[str]
    eta_to_next_stop_sec: Optional[int]
    lateness_min: Optional[int]
    deviation_score: Optional[float]
    predicted_arrival: Optional[str]
    next_stop_window_start: Optional[str]
    next_stop_window_end: Optional[str]
    window_shift_days: Optional[int]

    severity_source: str


class TelematicsSimulator:
    def __init__(self, route_points: Optional[Sequence[Tuple[float, float]]], baseline_duration_sec: int) -> None:
        self.route_points: List[Tuple[float, float]] = list(route_points) if route_points else self._generate_straight_route()

        # Optional improvement: if OSRM returns very few points, densify using straight-line fallback
        try:
            if route_points and len(self.route_points) < 20:
                print(f"⚠️ OSRM returned only {len(self.route_points)} points — densifying route to straight-line.")
                self.route_points = self._generate_straight_route(num_points=100)
        except Exception:
            # Be conservative: if anything goes wrong, keep original points
            pass

        self.total_points = len(self.route_points)

        # Ensure the route direction starts near START_COORD_LONLAT.
        if self.total_points >= 2:
            start_lat, start_lon = START_COORD_LONLAT[1], START_COORD_LONLAT[0]
            end_lat, end_lon = END_COORD_LONLAT[1], END_COORD_LONLAT[0]
            first_lat, first_lon = self.route_points[0]
            dist_first_to_start = haversine_m(first_lat, first_lon, start_lat, start_lon)
            dist_first_to_end = haversine_m(first_lat, first_lon, end_lat, end_lon)
            if dist_first_to_end < dist_first_to_start:
                self.route_points.reverse()

        # For demo time-compressed trips override baseline duration
        try:
            self.baseline_duration_sec = TRIP_DURATION_SEC
        except Exception:
            self.baseline_duration_sec = baseline_duration_sec if baseline_duration_sec > 0 else 30 * 60
        self.route_total_m = self._compute_route_total_m(self.route_points)
        self.traveled_m = 0.0

        self.eta_sec = int(self.baseline_duration_sec)
        self.prev_eta_sec: Optional[int] = None

        self.v_ema_mps = 0.0
        self.v_ema_alpha = 0.25

        self.start_time = time.time()
        self.task_state = "en_route"

        self.dtc_active: set[str] = set()
        self.engine_on = True

        self.gps_signal_lost = False
        self.gps_loss_duration = 0
        self.gps_loss_timer = 0
        self.in_deviation_window = False
        self.deviation_offset_east_m = 0.0

        self.distance_traveled_ratio = 0.0
        self.last_event_sent_ts = 0.0
        self.window_deviation_seconds = 0

        # incident lifecycle for dedupe
        self.active_incident_id: Optional[str] = None

        # stop cache
        self.plan_id: Optional[str] = None
        self.stops_cache: List[Dict[str, Any]] = []
        self.last_stop_refresh_ts = 0.0

    @staticmethod
    def _generate_straight_route(num_points: int = 100) -> List[Tuple[float, float]]:
        start_lat, start_lon = 47.6062, -122.3321
        end_lat, end_lon = 47.2529, -122.4443
        pts: List[Tuple[float, float]] = []
        for i in range(num_points):
            ratio = i / (num_points - 1)
            lat = start_lat + ratio * (end_lat - start_lat)
            lon = start_lon + ratio * (end_lon - start_lon)
            pts.append((lat, lon))
        return pts

    @staticmethod
    def _compute_route_total_m(pts: Sequence[Tuple[float, float]]) -> float:
        if len(pts) < 2:
            return 1.0
        total = 0.0
        for i in range(1, len(pts)):
            lat1, lon1 = pts[i - 1]
            lat2, lon2 = pts[i]
            total += haversine_m(lat1, lon1, lat2, lon2)
        return max(1.0, total)

    def _should_deviate(self, elapsed_sec: float) -> bool:
        if DISABLE_DEVIATION:
            return False
        return DEVIATE_AFTER_SEC <= elapsed_sec <= (DEVIATE_AFTER_SEC + DEVIATE_DURATION_SEC)

    def _position_by_ratio(self, ratio: float) -> Tuple[float, float]:
        if self.total_points <= 1:
            return self.route_points[0]

        r = max(0.0, min(1.0, ratio))
        f = r * (self.total_points - 1)

        i = int(math.floor(f))
        j = min(i + 1, self.total_points - 1)
        t = f - i

        lat1, lon1 = self.route_points[i]
        lat2, lon2 = self.route_points[j]

        lat = lat1 + (lat2 - lat1) * t
        lon = lon1 + (lon2 - lon1) * t

        return lat, lon

    def _update_task_state(self) -> None:
        if self.distance_traveled_ratio >= 1.0:
            self.task_state = "delivered"
        elif self.distance_traveled_ratio > 0.9:
            self.task_state = "approaching"
        else:
            self.task_state = "en_route"

    def _compute_true_heading(self, planned_lat: float, planned_lon: float) -> float:
        idx = min(int(self.distance_traveled_ratio * (self.total_points - 1)), self.total_points - 1)
        next_idx = min(idx + 1, self.total_points - 1)
        next_lat, next_lon = self.route_points[next_idx]
        dy = next_lat - planned_lat
        dx = next_lon - planned_lon
        return math.degrees(math.atan2(dx, dy)) % 360

    def _base_speed_kmh(self) -> float:
        base = 120.0  # 提高到120 km/h
        if self.distance_traveled_ratio < 0.1:
            base = 60.0 + 60.0 * (self.distance_traveled_ratio / 0.1)
        elif self.distance_traveled_ratio > 0.9:
            base = 120.0 * ((1.0 - self.distance_traveled_ratio) / 0.1)
        return max(0.0, base)

    def _simulate_gps(self, true_lat: float, true_lon: float, true_speed_kmh: float, true_heading_deg: float) -> Tuple[float, float, float, Optional[float]]:
        if not self.in_deviation_window:
            if (not self.gps_signal_lost) and random.random() < 0.02:
                self.gps_signal_lost = True
                self.gps_loss_duration = random.randint(3, 10)
                self.gps_loss_timer = 0
            elif self.gps_signal_lost:
                self.gps_loss_timer += STEP_INTERVAL_SEC
                if self.gps_loss_timer >= self.gps_loss_duration:
                    self.gps_signal_lost = False
        else:
            self.gps_signal_lost = False

        # Reduced GPS noise for demo to avoid map jitter
        noise_lat = random.gauss(0, 0.000003)
        noise_lon = random.gauss(0, 0.000003)
        noise_speed = random.gauss(0, 1.5)
        noise_heading = random.gauss(0, 8)

        lat = true_lat + noise_lat
        lon = true_lon + noise_lon
        speed = max(0.0, true_speed_kmh + noise_speed)
        heading = (true_heading_deg + noise_heading) % 360 if true_speed_kmh > 5 else None
        return round(lat, 6), round(lon, 6), round(speed, 1), (round(heading, 1) if heading is not None else None)

    def _generate_can(self, gps_speed_kmh: float) -> Tuple[float, List[str], str]:
        base = gps_speed_kmh if gps_speed_kmh is not None else 0.0
        can_speed = base + random.gauss(0, 0.8)

        if random.random() < 0.003:
            can_speed = 0.0

        if (not self.dtc_active) and random.random() < 0.008:
            if gps_speed_kmh and gps_speed_kmh > 100:
                self.dtc_active.add("P0087")
            elif self.distance_traveled_ratio > 0.7:
                self.dtc_active.add("P0171")
        elif self.dtc_active and random.random() < 0.005:
            self.dtc_active.clear()

        return round(can_speed, 1), list(self.dtc_active), ("ON" if self.engine_on else "OFF")

    def _update_eta(self, can_speed_kmh: float) -> Tuple[int, Optional[int], Optional[int]]:
        # Make ETA follow the time-compressed trip linearly
        self.prev_eta_sec = int(self.eta_sec) if self.eta_sec is not None else None
        self.eta_sec = int((1.0 - self.distance_traveled_ratio) * float(self.baseline_duration_sec))
        eta_delta_sec = (self.eta_sec - self.prev_eta_sec) if self.prev_eta_sec is not None else None
        return self.eta_sec, self.prev_eta_sec, eta_delta_sec

    def _refresh_stops_cache_if_needed(self) -> None:
        now_ts = time.time()
        if (now_ts - self.last_stop_refresh_ts) < STOP_REFRESH_SEC and self.stops_cache:
            return

        try:
            conn = db_conn()
            try:
                self.plan_id = fetch_active_plan_id(conn, VEHICLE_ID)
                if not self.plan_id:
                    self.stops_cache = []
                    return
                self.stops_cache = fetch_stops_for_plan(conn, self.plan_id, VEHICLE_ID)
                self.last_stop_refresh_ts = now_ts
            finally:
                conn.close()
        except Exception as e:
            print(f"⚠️ DB stop refresh failed: {type(e).__name__}: {e}")

    def step(self) -> TelemetryPayload:
        now_ts = time.time()
        elapsed = now_ts - self.start_time
        dt = STEP_INTERVAL_SEC

        base_speed_kmh = self._base_speed_kmh()
        self.in_deviation_window = self._should_deviate(elapsed)

        # 在偏离窗口中减速
        if self.in_deviation_window:
            base_speed_kmh = max(SLOWDOWN_MIN_KMH, base_speed_kmh * SLOWDOWN_FACTOR)

        # Time-compressed progression: complete trip in TRIP_DURATION_SEC seconds
        desired_mps = self.route_total_m / float(TRIP_DURATION_SEC)
        self.traveled_m = min(self.route_total_m, self.traveled_m + desired_mps * dt)
        self.distance_traveled_ratio = min(1.0, self.traveled_m / self.route_total_m)

        planned_lat, planned_lon = self._position_by_ratio(self.distance_traveled_ratio)

        if self.in_deviation_window:
            # Smoothly ramp into deviation to avoid a sudden jump.
            target_offset = float(DEVIATE_EAST_METERS)
            self.deviation_offset_east_m = (self.deviation_offset_east_m * 0.8) + (target_offset * 0.2)
        elif abs(self.deviation_offset_east_m) > 0.5:
            # Smoothly decay back to the planned route.
            self.deviation_offset_east_m *= 0.85
        else:
            self.deviation_offset_east_m = 0.0

        true_lat, true_lon = offset_meters(planned_lat, planned_lon, east_m=self.deviation_offset_east_m)
        true_heading = self._compute_true_heading(planned_lat, planned_lon)

        # 生成GPS/CAN信号
        lat, lon, gps_speed_kmh, heading = self._simulate_gps(true_lat, true_lon, base_speed_kmh, true_heading)
        can_speed_kmh, dtc_codes, engine_status = self._generate_can(gps_speed_kmh)

        # 计算偏离距离
        deviation_m = haversine_m(true_lat, true_lon, planned_lat, planned_lon)

        eta_sec, prev_eta_sec, eta_delta_sec = self._update_eta(can_speed_kmh)
        self._update_task_state()

        # ========= read next stop from DB =========
        self._refresh_stops_cache_if_needed()
        next_stop = pick_target_stop(self.stops_cache) if self.stops_cache else None


        next_stop_id = None
        eta_to_next_stop_sec = None
        lateness_min = None
        deviation_score = None
        predicted_arrival_str = None
        window_start_str = None
        window_end_str = None
        window_shift_days = None

        # ========= window-driven deviation =========
        is_dev = False
        if next_stop and next_stop.get("lat") is not None and next_stop.get("lon") is not None:
            next_stop_id = next_stop["stop_id"]
            dist_to_next_m = haversine_m(lat, lon, next_stop["lat"], next_stop["lon"])

            # If arrived at target stop, advance DB statuses and refresh cache
            if self.plan_id and dist_to_next_m < STOP_ARRIVAL_RADIUS_M:
                try:
                    conn2 = db_conn()
                    try:
                        advance_stop_status_if_arrived(conn2, self.plan_id, VEHICLE_ID, next_stop["stop_id"])
                        conn2.commit()
                    finally:
                        conn2.close()
                    # refresh stops after advancing
                    self.stops_cache = []
                    self.last_stop_refresh_ts = 0.0
                except Exception as e:
                    print(f"⚠️ stop advance failed: {type(e).__name__}: {e}")


            # ETA to next stop
            eta_to_next_stop_sec = int(dist_to_next_m / max(0.5, self.v_ema_mps))

            # DB windows (TIMESTAMP) -> naive datetimes
            w_start = next_stop.get("current_time_start") or next_stop.get("planned_time_start")
            w_end = next_stop.get("current_time_end") or next_stop.get("planned_time_end")

            if w_start and w_end:
                now_dt = now_for_db_compare()
                predicted_arrival = now_dt + timedelta(seconds=int(eta_to_next_stop_sec))
                predicted_arrival_str = predicted_arrival.isoformat()

                # 方案B：滚动对齐窗口到最近未来一天
                w_start_aligned, w_end_aligned, shift_days = align_window_to_now(
                    w_start, w_end, now_dt, past_grace_sec=2 * 3600, max_shift_days=7
                )
                window_shift_days = shift_days
                window_start_str = w_start_aligned.isoformat()
                window_end_str = w_end_aligned.isoformat()

                lateness_raw = compute_lateness_min(predicted_arrival, w_end_aligned)
                # 🔧 限制 lateness 上限为 45 分钟，避免窗口对齐问题
                lateness_min = min(lateness_raw, 45)

                # 如果窗口太靠后导致 lateness=0，但车辆已经偏离，
                # 用 ETA 增量作为“迟到感知”的替代（仅在 eta 变大时）
                if lateness_min == 0 and deviation_m > 0 and eta_delta_sec and eta_delta_sec > 0:
                    lateness_min = max(1, int(round(eta_delta_sec / 60)))

                # deviation_score = lateness / window_width
                window_width_min = max(10, int(round((w_end_aligned - w_start_aligned).total_seconds() / 60)))
                deviation_score = round(float(lateness_min) / float(window_width_min), 4)

                # 调试：打印窗口信息（仅在偏离时）
                if self.window_deviation_seconds > 0 and self.window_deviation_seconds % 10 == 0:
                    print(f"\n🔍 DEBUG Window: width={window_width_min}min, "
                          f"predicted={predicted_arrival.strftime('%H:%M:%S')}, "
                          f"window_end={w_end_aligned.strftime('%H:%M:%S')}, "
                          f"lateness_raw={lateness_raw}min → capped={lateness_min}min")

                # 🔧 优化 is_dev 判断：主要依赖物理偏离
                physical_dev = deviation_m > 50  # 物理偏离超过50米
                time_based_dev = (lateness_raw < 120) and (predicted_arrival > (w_end_aligned + timedelta(minutes=int(LATE_GRACE_MIN))))
                is_dev = physical_dev or time_based_dev

                if is_dev:
                    self.window_deviation_seconds += STEP_INTERVAL_SEC
                else:
                    self.window_deviation_seconds = 0

        # ========= incident event generation =========
        incident_id = None
        event_type = None
        event_severity = None
        eta_impact_min = None
        requires_reorder = None

        # 修改条件：必须同时满足时间偏离(is_dev) 和 物理偏离(self.in_deviation_window)
        if is_dev and self.in_deviation_window and (self.window_deviation_seconds >= OFFROUTE_TRIGGER_SEC) and ((now_ts - self.last_event_sent_ts) >= EVENT_THROTTLE_SEC):
            event_type = "ROUTE_MISMATCH"
            event_severity = severity_from_lateness_min(lateness_min or 0)

            # Keep a stable incident_id while active (dedupe)
            if not self.active_incident_id:
                self.active_incident_id = str(uuid.uuid4())
            incident_id = self.active_incident_id

            # ETA impact scaling by severity (demo)
            if event_severity == "LOW":
                pct = 0.03
            elif event_severity == "MEDIUM":
                pct = ETA_IMPACT_PCT_MEDIUM
            else:
                pct = 0.10  # keep HIGH impact but not crazy

            remaining_sec = int(eta_sec) if eta_sec is not None else int(self.baseline_duration_sec)
            eta_impact_min = max(1, round((remaining_sec * pct) / 60))

            requires_reorder = event_severity in ("MEDIUM", "HIGH")
            self.last_event_sent_ts = now_ts

        # If deviation clears, close out active incident id
        if not is_dev and self.window_deviation_seconds == 0:
            self.active_incident_id = None

        # Enforce global disable for incidents (demo mode)
        if DISABLE_INCIDENTS:
            incident_id = None
            event_type = None
            event_severity = None
            eta_impact_min = None
            requires_reorder = False

        return TelemetryPayload(
            vehicle_id=VEHICLE_ID,
            timestamp=now_ts,
            latitude=lat,
            longitude=lon,
            location={"lat": lat, "lon": lon},
            speed=gps_speed_kmh,
            heading=heading,
            CAN_speed=can_speed_kmh,
            DTC_codes=dtc_codes,
            engine_status=engine_status,
            task_state=self.task_state,
            planned_latitude=round(planned_lat, 6),
            planned_longitude=round(planned_lon, 6),
            deviation_m=round(deviation_m, 1),
            route_baseline_duration_sec=int(self.baseline_duration_sec),
            distance_ratio=round(self.distance_traveled_ratio, 4),
            route_total_m=round(self.route_total_m, 1),
            traveled_m=round(self.traveled_m, 1),
            eta_sec=int(eta_sec) if eta_sec is not None else None,
            prev_eta_sec=int(prev_eta_sec) if prev_eta_sec is not None else None,
            eta_delta_sec=int(eta_delta_sec) if eta_delta_sec is not None else None,
            incident_id=incident_id,
            event_type=event_type,
            event_severity=event_severity,
            eta_impact_min=eta_impact_min,
            requires_reorder=requires_reorder,
            next_stop_id=next_stop_id,
            eta_to_next_stop_sec=int(eta_to_next_stop_sec) if eta_to_next_stop_sec is not None else None,
            lateness_min=int(lateness_min) if lateness_min is not None else None,
            deviation_score=float(deviation_score) if deviation_score is not None else None,
            predicted_arrival=predicted_arrival_str,
            next_stop_window_start=window_start_str,
            next_stop_window_end=window_end_str,
            window_shift_days=int(window_shift_days) if window_shift_days is not None else None,
            severity_source="simulator",
        )


# =========================
# Main loop
# =========================

def main() -> None:
    global PLAN_ID_GLOBAL
    
    # 设置Ctrl+C处理器
    signal.signal(signal.SIGINT, cleanup_handler)
    
    print("📍 Loading route from OSRM...")
    route, baseline_duration_sec = fetch_real_route(OSRM_ROUTER, START_COORD_LONLAT, END_COORD_LONLAT)

    sim = TelematicsSimulator(route_points=route, baseline_duration_sec=baseline_duration_sec)
    session = build_retry_session()

    # Force initial DB stop load to populate sim.plan_id and stops_cache
    sim._refresh_stops_cache_if_needed()

    # 备份原始stop状态
    try:
        conn = db_conn()
        try:
            PLAN_ID_GLOBAL = sim.plan_id
            if PLAN_ID_GLOBAL:
                backup_stops_status(conn, PLAN_ID_GLOBAL, VEHICLE_ID)
                # 注册退出时的恢复函数
                atexit.register(restore_stops_status, PLAN_ID_GLOBAL, VEHICLE_ID)
                print(f"📋 Active plan: {PLAN_ID_GLOBAL}")
        finally:
            conn.close()
    except Exception as e:
        print(f"⚠️ Failed to backup stops: {e}")

    consecutive_fail = 0
    consecutive_ok = 0
    backoff_mult = 1
    last_reroute_incident_id: Optional[str] = None

    print("🚀 Simulator running. Sending data every 2 seconds...\n")

    while True:
        payload = sim.step()

        send_ok = False
        resp = None
        err: Optional[Exception] = None

        try:
            resp = session.post(
                API_URL,
                json=payload.__dict__,
                timeout=(CONNECT_TIMEOUT_SEC, READ_TIMEOUT_SEC),
            )
            send_ok = (resp.status_code == 200)
        except requests.exceptions.RequestException as exc:
            err = exc
            send_ok = False

        if send_ok:
            consecutive_ok += 1
            consecutive_fail = 0
            if consecutive_ok >= SUCCESS_TO_RESET_BACKOFF:
                backoff_mult = 1
        else:
            consecutive_fail += 1
            consecutive_ok = 0
            if consecutive_fail >= FAILS_TO_INCREASE_BACKOFF:
                backoff_mult = min(SEND_BACKOFF_MAX_MULT, backoff_mult * 2)

        status = "✅" if send_ok else (f"⚠️ {resp.status_code}" if resp is not None else "❌")

        eta_part = f"ETA(prev/curr/delta)s={payload.prev_eta_sec}/{payload.eta_sec}/{payload.eta_delta_sec}"

        debug_part = ""
        if payload.next_stop_id:
            debug_part = (
                f" | NEXT={payload.next_stop_id} eta_to_next={payload.eta_to_next_stop_sec}s "
                f"late={payload.lateness_min}m score={payload.deviation_score} shift={payload.window_shift_days}d"
            )

        event_part = ""
        if payload.event_type:
            event_part = (
                f" | EVENT={payload.event_type} sev={payload.event_severity} "
                f"eta_impact_min={payload.eta_impact_min} reorder={payload.requires_reorder} incident_id={payload.incident_id}"
            )
        
        reason_part = ""
        if not send_ok and resp is None and err is not None:
            reason_part = f" | err={type(err).__name__}: {err}"

        backoff_part = f" | backoff=x{backoff_mult}" if backoff_mult > 1 else ""

        print(
            f"[{time.strftime('%H:%M:%S')}] {status} "
            f"Dev={payload.deviation_m}m WinDevSec={sim.window_deviation_seconds} "
            f"Lat={payload.latitude}, Speed(GPS/CAN)={payload.speed}/{payload.CAN_speed}, "
            f"DTCs={payload.DTC_codes}, Task={payload.task_state} | {eta_part}"
            f"{debug_part}{event_part}{reason_part}{backoff_part}"
        )

        # Trigger reroute API (Lambda) and apply stop resequence to DB
        # 基于物理偏离触发：偏离>40m 且持续>OFFROUTE_TRIGGER_SEC
        # 适配测试值 55/80/120m
        has_significant_deviation = (
            payload.incident_id and 
            payload.deviation_m and payload.deviation_m > 40 and
            sim.window_deviation_seconds >= OFFROUTE_TRIGGER_SEC
        )

        # Global override to prevent any reroute/incidents during demo
        if DISABLE_INCIDENTS:
            has_significant_deviation = False
        
        if has_significant_deviation:
            if payload.incident_id and payload.incident_id == last_reroute_incident_id:
                # 同一个 incident 只触发一次 reroute
                time.sleep(STEP_INTERVAL_SEC * backoff_mult)
                continue
            print(f"\n🚨 INCIDENT DETECTED! severity={payload.event_severity}, lateness={payload.lateness_min}min, "
                  f"deviation_score={payload.deviation_score}")
            try:
                # Ensure we have freshest stops before reroute
                sim._refresh_stops_cache_if_needed()
                if not sim.plan_id or not sim.stops_cache:
                    print("  🔄 [REROUTE] ⚠️ No active plan/stops in DB; skip")
                else:
                    # Print BEFORE state
                    print(f"\n📦 BEFORE REROUTE (incident_id={payload.incident_id}):")
                    for s in sim.stops_cache:
                        if s["status"] != "DONE":
                            print(f'  {s["stop_id"]}: seq={s["current_sequence"]} status={s["status"]}')
                    
                    # Build remaining_stops from DB cache (only PENDING / IN_PROGRESS)
                    remaining = []
                    for s in sim.stops_cache:
                        st = (s.get("status") or "").upper()
                        if st in ("PENDING", "IN_PROGRESS"):
                            if s.get("lat") is None or s.get("lon") is None:
                                continue
                            remaining.append({
                                "stop_id": s["stop_id"],
                                "location": {"lat": float(s["lat"]), "lon": float(s["lon"])}
                            })

                    reroute_payload = {
                        "vehicle_id": VEHICLE_ID,
                        "eta_sec": int(payload.eta_sec) if payload.eta_sec is not None else 1800,
                        "requires_reorder": True,
                        "remaining_stops": remaining,
                        "location": {"lat": float(payload.latitude), "lon": float(payload.longitude)},
                        "debug_source": "SIMULATOR_REROUTE"
                    }

                    print(f"  📤 Sending to Lambda:")
                    print(f"     - vehicle_id: {VEHICLE_ID}")
                    print(f"     - eta_sec: {reroute_payload['eta_sec']}")
                    print(f"     - location: {reroute_payload['location']}")
                    print(f"     - num_stops: {len(remaining)}")
                    print(f"     - lateness_min: {payload.lateness_min}")

                    reroute_resp = session.post(
                        API_URL_Reroute,
                        json=reroute_payload,
                        timeout=(CONNECT_TIMEOUT_SEC, READ_TIMEOUT_SEC),
                    )

                    if reroute_resp.status_code != 200:
                        print(f"  🔄 [REROUTE] ⚠️ HTTP {reroute_resp.status_code}: {reroute_resp.text[:500]}")
                        # 打印完整 payload 用于调试
                        import json
                        print(f"  📋 Full payload sent:")
                        print(json.dumps(reroute_payload, indent=2))
                    else:
                        decision_data = reroute_resp.json()
                        decision = decision_data.get("decision")
                        stop_order = decision_data.get("stop_order")

                        print(f"  🔄 [REROUTE] ✅ decision={decision} stop_order={stop_order}")

                        # Build old sequence from current cache (BEFORE state)
                        old_sequence = [s["stop_id"] for s in sim.stops_cache if s["status"] != "DONE"]
                        # new sequence from lambda decision
                        new_sequence = stop_order

                        # Apply to DB if reroute accepted and stop_order present
                        if decision == "REROUTE" and stop_order and sim.plan_id:
                            conn3 = db_conn()
                            try:
                                # 1) persist reroute result (survives restore)
                                record_stop_resequence_result(conn3, sim.plan_id, payload.incident_id, old_sequence, new_sequence)

                                # 2) apply to DB current_sequence
                                apply_stop_order_to_db(conn3, sim.plan_id, VEHICLE_ID, stop_order)
                                conn3.commit()
                            finally:
                                conn3.close()

                            # 记录已处理的 incident，避免重复 reroute
                            last_reroute_incident_id = payload.incident_id

                            # Force refresh cache so next step uses new sequence
                            sim.stops_cache = []
                            sim.last_stop_refresh_ts = 0.0
                            sim._refresh_stops_cache_if_needed()

                            print(f"  ✅ [DB UPDATED] Applied new current_sequence order")
                            print(f"\n🔁 AFTER REROUTE:")
                            for s in sim.stops_cache:
                                if s["status"] != "DONE":
                                    print(f'  {s["stop_id"]}: seq={s["current_sequence"]} status={s["status"]}')
                            print()

            except Exception as e:
                print(f"  🔄 [REROUTE] ❌ {type(e).__name__}: {e}")

        time.sleep(STEP_INTERVAL_SEC * backoff_mult)


if __name__ == "__main__":
    main()
