# app/thresholds.py

from __future__ import annotations
import json
import logging
from datetime import timedelta
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Dict, Optional
import optuna

from data.cache_handler import SimpleCacheHandler

log = logging.getLogger("thresholds")
log.setLevel(logging.WARNING)

# ──────────────────────────── Константи ──────────────────────────────
CACHE_TTL_DAYS: int = 14           # скільки днів тримати пороги в Redis
OPTUNA_SQLITE_URI = "sqlite:///storage/optuna.db"   # база Optuna (Heat-map, Dashboard)

# ══════════════════════════ SQLite-історія (опційно) ═════════════════════════
# import sqlite3
# DB_PATH = Path("storage/thresholds_history.db")
#
# def _get_conn() -> sqlite3.Connection:
#     """Повертає SQLite-коннектор, створюючи таблицю, якщо її ще нема."""
#     DB_PATH.parent.mkdir(parents=True, exist_ok=True)
#     conn = sqlite3.connect(DB_PATH)
#     conn.execute(
#         """
#         CREATE TABLE IF NOT EXISTS thresholds_history (
#             id           INTEGER PRIMARY KEY AUTOINCREMENT,
#             symbol       TEXT NOT NULL,
#             tuned_at     INTEGER NOT NULL,
#             payload_json TEXT NOT NULL,
#             study_uuid   TEXT,
#             comment      TEXT
#         )
#         """
#     )
#     return conn
# ══════════════════════════════════════════════════════════════════════════════


# ────────────────────────────── Dataclass ─────────────────────────────────────
@dataclass
class Thresholds:
    """Порогові значення для сигналів одного символу.

    Attributes
    ----------
    low_gate : float
        Нижня межа ATR/price для «тихого» ринку (0.004 = 0.4 %).
    high_gate : float
        Верхня межа ATR/price для «високої» волатильності.
    atr_target : float
        Множник ATR для take-profit (0.5 = 50 % ATR).
    vol_z_threshold : float
        Поріг Z-score обсягу (сплески обсягу).
    """
    low_gate: float = 0.006        # 0.6 %
    high_gate: float = 0.015       # 1.5 %
    atr_target: float = 0.50
    vol_z_threshold: float = 1.2

    # ───── Автокорекція точності ─────
    def __post_init__(self) -> None:
        self.low_gate = round(self.low_gate, 4)                 # low_gate, high_gate: до 4 знаків після крапки (0.0001 precision)
        self.high_gate = round(self.high_gate, 4)
        self.atr_target = round(self.atr_target, 2)             # atr_target: до 2 знаків після крапки
        self.vol_z_threshold = round(self.vol_z_threshold, 1)   # vol_z_threshold: до 1 знака після крапки

    # ───── Утиліти ─────
    @classmethod
    def from_mapping(cls, data: Dict) -> "Thresholds":
        """Створює Thresholds із mapping, ігноруючи зайві ключі."""
        allowed = {k: data[k] for k in cls.__annotations__.keys() if k in data}
        return cls(**allowed)


# ───────────────────────────── Redis-ключ ─────────────────────────────────────
def _redis_key(symbol: str) -> str:
    """Формує ключ у Redis для порогів символу."""
    return f"thresholds:{symbol}"


# ───────────────────────────── Збереження ─────────────────────────────────────
async def save_thresholds(
    symbol: str,
    thr: Thresholds,
    cache: SimpleCacheHandler,
) -> None:
    """Зберігає Thresholds у Redis (+ JSON-бек-ап)."""
    key = _redis_key(symbol)
    payload = json.dumps(asdict(thr), ensure_ascii=False)

    # 1) Redis
    await cache.store_in_cache(
        key,
        "global",
        payload,
        ttl=timedelta(days=CACHE_TTL_DAYS),
        raw=True,
    )

    # 2) Локальний резерв (dev-mode, не впливає на Heroku slug)
    try:
        Path("optuna_runs").mkdir(exist_ok=True)
        (Path("optuna_runs") / f"{symbol}.json").write_text(
            json.dumps({"best_params": asdict(thr)}, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception as exc:  # noqa: BLE001
        # Лише попередження — не критично у production
        print(f"[WARN] Cannot write local backup for {symbol}: {exc}")


async def save_thresholds_version(
    cache: SimpleCacheHandler,
    symbol: str,
    thr: Thresholds,
    *,
    study_uuid: Optional[str] = None,
    comment: str = "",
    ttl_days: int = CACHE_TTL_DAYS,
) -> None:
    """Запис «живих» порогів + (опційно) версію в SQLite-історію."""
    # Redis
    await cache.store_in_cache(
        _redis_key(symbol),
        "global",
        json.dumps(asdict(thr)),
        ttl=timedelta(days=ttl_days),
        raw=True,
    )

    # SQLite — вимкнено, залишено для майбутнього
    # conn = _get_conn()
    # with conn:
    #     conn.execute(
    #         "INSERT INTO thresholds_history "
    #         "(symbol, tuned_at, payload_json, study_uuid, comment) "
    #         "VALUES (?,?,?,?,?)",
    #         (
    #             symbol,
    #             int(time.time()),
    #             json.dumps(asdict(thr)),
    #             study_uuid,
    #             comment,
    #         ),
    #     )


# ───────────────────────────── Завантаження ──────────────────────────────────
async def load_thresholds(
    symbol: str,
    cache: SimpleCacheHandler,
) -> Thresholds:
    """
    Завантажує Thresholds з Redis. Якщо ключа нема або JSON некоректний,
    повертає дефолтні значення Thresholds().

    Params:
        symbol – ticker, наприклад "BTCUSDT"
        cache  – екземпляр SimpleCacheHandler для взаємодії з Redis
    """
    key = _redis_key(symbol)
    raw = await cache.fetch_from_cache(key, "global", raw=True)
    if not raw:
        log.debug("[%s] load_thresholds: ключ %s відсутній у Redis, використовуємо дефолт",
                  symbol, key)
        return Thresholds()

    # raw може бути bytes або str
    raw_str = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw
    log.debug("[%s] load_thresholds: знайдено в Redis: %s...", symbol, raw_str[:100])
    try:
        data = json.loads(raw_str)
        thr = Thresholds.from_mapping(data)
        log.debug("[%s] load_thresholds: повертаємо з Redis %s", symbol, thr)
        return thr
    except Exception as exc:
        log.warning("[%s] load_thresholds: не вдалося розпарсити JSON (%s), використовуємо дефолт",
                    symbol, exc)
        return Thresholds()


# ──────────────────────── (опціонально) вибір за часом ───────────────────────
# def load_thresholds_by_time(symbol: str, ts: int) -> Thresholds | None:
#     """Повертає останні пороги *до* зазначеної мітки часу."""
#     conn = _get_conn()
#     row = conn.execute(
#         """
#         SELECT payload_json
#         FROM   thresholds_history
#         WHERE  symbol=? AND tuned_at<=?
#         ORDER  BY tuned_at DESC
#         LIMIT  1
#         """,
#         (symbol, ts),
#     ).fetchone()
#     return Thresholds.from_mapping(json.loads(row[0])) if row else None


# ───────────────────────────── Optuna Study ───────────────────────────────────
def get_study(study_name: str) -> optuna.study.Study:
    """Створює/витягує Optuna Study (SQLite-storage)."""
    return optuna.create_study(
        study_name=study_name,
        directions=["maximize", "minimize"],  # multi-objective (прибуток, drawdown)
        storage=OPTUNA_SQLITE_URI,
        load_if_exists=True,
    )
