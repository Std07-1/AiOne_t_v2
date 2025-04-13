# order_book_handler.py

import os
import sys
import pandas as pd
import requests
import logging
import time
import json
import io
from collections import defaultdict
from typing import Optional, Dict, Any

# Не забудьте імпортувати Redis, якщо потрібен (залежно від вашої реалізації CacheHandler)
# from redis.asyncio import Redis
from datafetcher.cache_handler import CacheHandler
from datafetcher.file_manager import FileManager
from datafetcher.utils import ColumnValidator

logger = logging.getLogger("OrderBookHandler")

# Дефолтні порогові значення (вони можуть бути налаштовані через параметри)
DEFAULT_LARGE_ORDER_THRESHOLD = (
    1000  # ордер вважаємо "великим", якщо кількість > 100000
)
DEFAULT_SPOOFING_THRESHOLD_RATIO = (
    0.05  # якщо сумарна зміна > 20% від загального обсягу – можливий spoofing
)
DEFAULT_HIDDEN_ORDER_THRESHOLD = 50  # поріг для "різниці" для hidden orders
DEFAULT_ICEBERG_APPEARANCES = (
    3  # кількість появ одного і того ж рівня для iceberg-патерну
)


class OrderBookHandler:
    def __init__(
        self,
        redis_client: Optional[Any] = None,
        file_manager: Optional[FileManager] = None,
        cache_handler: Optional[CacheHandler] = None,
        validate: Optional[ColumnValidator] = None,
        cache_ttl: int = 3600,
    ):
        self.cache_handler = cache_handler or CacheHandler(
            redis_clients=[redis_client] if redis_client else []
        )
        self.file_manager = file_manager or FileManager()
        self.validate = validate or ColumnValidator()
        self.cache_ttl = cache_ttl
        # Для збереження історичних снапшотів (опціонально)
        self.snapshot_list = []

    def _get_cache_key(self, symbol: str) -> str:
        return f"order_book:{symbol}"

    def fetch_order_book_api(
        self, symbol: str, limit: int = 100
    ) -> Optional[Dict[str, pd.DataFrame]]:
        """
        Отримує дані ордер книги через REST API Binance.
        Повертає словник з DataFrame для bids та asks.
        """
        url = "https://api.binance.com/api/v3/depth"
        params = {"symbol": symbol, "limit": limit}
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            bids = pd.DataFrame(
                data["bids"], columns=["price", "quantity"], dtype=float
            )
            asks = pd.DataFrame(
                data["asks"], columns=["price", "quantity"], dtype=float
            )

            logger.debug(
                f"Отримано ордер книгу для {symbol}: {len(bids)} bids, {len(asks)} asks"
            )

            bids["side"] = "bid"
            asks["side"] = "ask"
            return {"bids": bids, "asks": asks}
        except Exception as e:
            logger.error(f"Error fetching order book for {symbol}: {e}", exc_info=True)
            return None

    async def store_order_book(self, symbol: str, order_book_df: pd.DataFrame) -> None:
        """
        Зберігає отримані дані ордер книги у кеші та файлі.
        """
        cache_key = self._get_cache_key(symbol)
        try:
            await self.cache_handler.store_in_cache(
                cache_key, order_book_df.to_dict(orient="records"), self.cache_ttl
            )
            logger.info(f"Order book for {symbol} stored in cache.")
        except Exception as e:
            logger.error(
                f"Error storing order book for {symbol} in cache: {e}", exc_info=True
            )
        try:
            self.file_manager.save_file(
                symbol, "order_book", "raw", order_book_df, "json"
            )
            logger.info(f"Order book for {symbol} saved to file.")
        except Exception as e:
            logger.error(
                f"Error saving order book for {symbol} to file: {e}", exc_info=True
            )

    async def get_cached_order_book(self, symbol: str) -> Optional[pd.DataFrame]:
        """
        Повертає кешовані дані ордер книги для символу (якщо є).
        """
        cache_key = self._get_cache_key(symbol)
        try:
            cached_data = await self.cache_handler.fetch_from_cache(cache_key)
            if cached_data:
                return pd.DataFrame(cached_data)
            return None
        except Exception as e:
            logger.error(
                f"Error fetching cached order book for {symbol}: {e}", exc_info=True
            )
            return None

    async def get_order_book(self, symbol: str, limit: int = 100) -> pd.DataFrame:
        """
        Основна функція отримання ордер книги:
          - Спроба отримати з кешу, інакше отримуємо через API,
          - Зберігаємо отримані дані (у кеш і файл) та повертаємо DataFrame.
        """
        cached = await self.get_cached_order_book(symbol)
        if cached is not None:
            logger.info(f"Using cached order book for {symbol}.")
            order_book_df = cached
        else:
            api_data = self.fetch_order_book_api(symbol, limit)
            if api_data is None:
                return pd.DataFrame()
            order_book_df = pd.concat(
                [api_data["bids"], api_data["asks"]], ignore_index=True
            )
            await self.store_order_book(symbol, order_book_df)
        # Зберігаємо снапшот для історичного аналізу
        self.snapshot_list.append(order_book_df.to_dict(orient="records"))
        return order_book_df

    async def process_order_book(self, symbol: str, limit: int = 100) -> Dict[str, Any]:
        """
        Основна функція для роботи з ордер книгою:
          - Отримуємо дані з кешу або через API,
          - Аналізуємо дані, зберігаємо результати аналізу та повертаємо їх.
        """
        order_book_df = await self.get_order_book(symbol, limit)
        analysis = self.analyze_order_book(order_book_df)
        try:
            analysis_df = pd.DataFrame([analysis])
            self.file_manager.save_file(
                symbol, "order_book", "analysis", analysis_df, "json"
            )
            logger.info(f"Order book analysis for {symbol} saved to file.")
        except Exception as e:
            logger.error(
                f"Error saving order book analysis for {symbol}: {e}", exc_info=True
            )
        return analysis

    def analyze_order_book(
        self, order_book_df: pd.DataFrame, snapshot_timestamp: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Розширений аналіз ордер книги:
          - Обчислення сумарних обсягів, зважених середніх цін.
          - Розрахунок дисбалансу обсягів та співвідношення.
          - Детальний аналіз spoofing та прихованих ордерів.
          - Виявлення появи/зникнення великих ордерів та iceberg-патернів.
          - Визначення можливих рухів маркетмейкерів та крупних гравців.
        """
        if order_book_df.empty:
            return {}
        try:
            # Розділяємо дані на BID та ASK
            bids = order_book_df[order_book_df["side"] == "bid"].copy()
            asks = order_book_df[order_book_df["side"] == "ask"].copy()

            # Обчислюємо сумарні обсяги
            total_bid_volume = bids["quantity"].sum()
            total_ask_volume = asks["quantity"].sum()

            # Обчислення зважених середніх цін
            bid_wap = (
                (bids["price"] * bids["quantity"]).sum() / total_bid_volume
                if total_bid_volume > 0
                else 0
            )
            ask_wap = (
                (asks["price"] * asks["quantity"]).sum() / total_ask_volume
                if total_ask_volume > 0
                else 0
            )

            # Дисбаланс обсягів та відношення
            imbalance = total_bid_volume - total_ask_volume
            imbalance_ratio = (
                (imbalance / (total_bid_volume + total_ask_volume))
                if (total_bid_volume + total_ask_volume) > 0
                else 0
            )
            logger.debug(
                f"Total BID volume: {total_bid_volume}, Total ASK volume: {total_ask_volume}"
            )

            # Аналіз spoofing – базовий
            threshold_ratio = DEFAULT_SPOOFING_THRESHOLD_RATIO
            sudden_bid_change = False
            sudden_ask_change = False
            if len(bids) > 1:
                sudden_bid_change = (
                    bids["quantity"].diff().abs().sum()
                    > threshold_ratio * total_bid_volume
                )
            if len(asks) > 1:
                sudden_ask_change = (
                    asks["quantity"].diff().abs().sum()
                    > threshold_ratio * total_ask_volume
                )
            spoofing_detected = sudden_bid_change or sudden_ask_change

            # Детальний аналіз spoofing
            bid_diff_sum = bids["quantity"].diff().abs().sum() if len(bids) > 1 else 0
            ask_diff_sum = asks["quantity"].diff().abs().sum() if len(asks) > 1 else 0
            spoofing_bid = bid_diff_sum > threshold_ratio * total_bid_volume
            spoofing_ask = ask_diff_sum > threshold_ratio * total_ask_volume
            spoofing_detected = spoofing_bid or spoofing_ask

            # Аналіз прихованих ордерів (базовий)
            hidden_bid_detected = False
            hidden_ask_detected = False
            if len(bids) > 1:
                bids["repeat_count"] = bids["quantity"].diff().abs()
                hidden_bid_detected = (
                    bids["repeat_count"] > DEFAULT_HIDDEN_ORDER_THRESHOLD
                ).any()
            if len(asks) > 1:
                asks["repeat_count"] = asks["quantity"].diff().abs()
                hidden_ask_detected = (
                    asks["repeat_count"] > DEFAULT_HIDDEN_ORDER_THRESHOLD
                ).any()

            # Розширений аналіз прихованих ордерів
            hidden_bid_count = (
                (bids["repeat_count"] > DEFAULT_HIDDEN_ORDER_THRESHOLD).sum()
                if "repeat_count" in bids.columns
                else 0
            )
            hidden_ask_count = (
                (asks["repeat_count"] > DEFAULT_HIDDEN_ORDER_THRESHOLD).sum()
                if "repeat_count" in asks.columns
                else 0
            )
            hidden_bid_ratio = hidden_bid_count / len(bids) if len(bids) > 0 else 0
            hidden_ask_ratio = hidden_ask_count / len(asks) if len(asks) > 0 else 0
            extended_hidden_orders = {
                "hidden_bid_count": int(hidden_bid_count),
                "hidden_bid_ratio": hidden_bid_ratio,
                "hidden_ask_count": int(hidden_ask_count),
                "hidden_ask_ratio": hidden_ask_ratio,
            }

            # Виявлення появи/зникнення великих ордерів (порівняння з попереднім снапшотом)
            large_orders_appeared = []
            large_orders_disappeared = []
            if hasattr(self, "snapshot_list") and len(self.snapshot_list) > 1:
                prev_snapshot = self.snapshot_list[-2]
                prev_bids = pd.DataFrame(
                    prev_snapshot.get("bids", []), columns=["price", "quantity"]
                )
                prev_asks = pd.DataFrame(
                    prev_snapshot.get("asks", []), columns=["price", "quantity"]
                )
                for _, row in bids.iterrows():
                    price, qty = row["price"], row["quantity"]
                    if (
                        prev_bids[prev_bids["price"] == price].empty
                        and qty > DEFAULT_LARGE_ORDER_THRESHOLD
                    ):
                        large_orders_appeared.append(("BID", price, qty))
                for _, row in asks.iterrows():
                    price, qty = row["price"], row["quantity"]
                    if (
                        prev_asks[prev_asks["price"] == price].empty
                        and qty > DEFAULT_LARGE_ORDER_THRESHOLD
                    ):
                        large_orders_appeared.append(("ASK", price, qty))
                for _, row in prev_bids.iterrows():
                    price, qty = row["price"], row["quantity"]
                    if (
                        bids[bids["price"] == price].empty
                        and qty > DEFAULT_LARGE_ORDER_THRESHOLD
                    ):
                        large_orders_disappeared.append(("BID", price, qty))
                for _, row in prev_asks.iterrows():
                    price, qty = row["price"], row["quantity"]
                    if (
                        asks[asks["price"] == price].empty
                        and qty > DEFAULT_LARGE_ORDER_THRESHOLD
                    ):
                        large_orders_disappeared.append(("ASK", price, qty))

            # Виявлення iceberg-патернів
            iceberg_detected = False
            iceberg_count_map = defaultdict(int)
            for side, price, qty in large_orders_appeared:
                key = f"{side}_{price}"
                iceberg_count_map[key] += 1
                if iceberg_count_map[key] >= DEFAULT_ICEBERG_APPEARANCES:
                    iceberg_detected = True
                    logger.warning(
                        f"Виявлено можливий iceberg-патерн: {side} {price}, повторів: {iceberg_count_map[key]}"
                    )

            # Аналіз рухів маркетмейкерів/крупних гравців
            market_maker_moves = False
            market_maker_details = []
            market_maker_threshold = 10 * DEFAULT_LARGE_ORDER_THRESHOLD
            for order in large_orders_appeared:
                if order[2] > market_maker_threshold:
                    market_maker_moves = True
                    market_maker_details.append(order)
            for order in large_orders_disappeared:
                if order[2] > market_maker_threshold:
                    market_maker_moves = True
                    market_maker_details.append(order)

            # Генерація підсумкового висновку про стан ринку
            if imbalance_ratio > 0.1:
                state = "Виявлено помірний/сильний попит (bullish)."
            elif imbalance_ratio < -0.1:
                state = "Виявлено помірний/сильний тиск продаж (bearish)."
            else:
                state = "Ринок збалансовано."
            if spoofing_detected:
                state += " Попередження: потенційна маніпуляція (spoofing)."
            if market_maker_moves:
                state += " Виявлено активність крупних гравців (market maker moves)."

            state_summary = {
                "total_bid_volume": total_bid_volume,
                "total_ask_volume": total_ask_volume,
                "bid_wap": bid_wap,
                "ask_wap": ask_wap,
                "volume_imbalance": imbalance,
                "imbalance_ratio": imbalance_ratio,
                "spoofing_detected": spoofing_detected,
                "hidden_bid_detected": hidden_bid_detected,
                "hidden_ask_detected": hidden_ask_detected,
                "extended_hidden_orders": extended_hidden_orders,
                "detailed_spoofing": spoofing_detected,
                "large_orders_appeared": large_orders_appeared,
                "large_orders_disappeared": large_orders_disappeared,
                "iceberg_detected": iceberg_detected,
                "market_maker_moves": {
                    "detected": market_maker_moves,
                    "details": market_maker_details,
                },
                "current_state": state,
                "snapshot_timestamp": snapshot_timestamp or int(time.time() * 1000),
            }

            logger.info(f"Order book analysis completed: {state_summary}")
            return state_summary

        except Exception as e:
            logger.error(f"Error analyzing order book: {e}", exc_info=True)
            return {}

    @staticmethod
    def order_book_report(
        symbol: str,
        order_book_data: Dict[str, Any],
        capital_movements: Dict[str, Any],
        stochastic_signals: Dict[str, Any],
        rsi_signals: Dict[str, Any],
        vwap: float,
        current_price: float,
        atr: float,
        deep_analysis: bool = False,
    ) -> str:
        """
        Формує адаптивний звіт на основі аналізу ордер книги, рухів капіталу та торгових індикаторів.
        """
        report_lines = []
        report_lines.append(
            f"**Детальний аналіз ордерної книги та рухів капіталу для {symbol}**:\n"
        )

        # Основні дані ордер книги
        bid_volume = order_book_data.get("total_bid_volume", 0)
        ask_volume = order_book_data.get("total_ask_volume", 0)
        imbalance = order_book_data.get("volume_imbalance", 0)
        imbalance_ratio = order_book_data.get("imbalance_ratio", 0)

        report_lines.append("**Обсяг BID/ASK:**")
        report_lines.append(f"- BID обсяг: {bid_volume:,.2f}")
        report_lines.append(f"- ASK обсяг: {ask_volume:,.2f}")
        report_lines.append(f"- Дисбаланс: {imbalance:,.2f}")
        if imbalance_ratio > 0:
            report_lines.append(
                f"- Співвідношення (BID-надлишок): {imbalance_ratio:.2f} (домінують покупці)"
            )
        elif imbalance_ratio < 0:
            report_lines.append(
                f"- Співвідношення (ASK-надлишок): {abs(imbalance_ratio):.2f} (домінують продавці)"
            )
        else:
            report_lines.append("- Співвідношення: Нейтрально")

        # Ключові рівні
        bid_wap = order_book_data.get("bid_wap", 0)
        ask_wap = order_book_data.get("ask_wap", 0)
        report_lines.append("\n**Ключові рівні:**")
        report_lines.append(f"- Зважена ціна BID: {bid_wap:,.2f}")
        report_lines.append(f"- Зважена ціна ASK: {ask_wap:,.2f}")
        report_lines.append(f"- Зона опору: {ask_wap:,.2f} (може стримувати зростання)")
        report_lines.append(
            f"- Зона підтримки: {bid_wap:,.2f} (може утримувати зниження)"
        )

        # Аналіз маніпуляцій
        report_lines.append("\n**Аналіз маніпуляцій:**")
        if order_book_data.get("spoofing_detected", False):
            report_lines.append(
                "- Виявлено потенційний spoofing (швидкі зміни обсягів)."
            )
        else:
            report_lines.append("- Spoofing не виявлено.")
        if order_book_data.get("hidden_bid_detected", False) or order_book_data.get(
            "hidden_ask_detected", False
        ):
            report_lines.append("- Виявлено приховані ордери (базовий аналіз).")
        else:
            report_lines.append("- Приховані ордери не виявлено.")

        # Розширений аналіз прихованих ордерів
        extended_hidden = order_book_data.get("extended_hidden_orders", {})
        if extended_hidden:
            report_lines.append("\n**Розширений аналіз прихованих ордерів:**")
            report_lines.append(
                f"- Кількість прихованих BID ордерів: {extended_hidden.get('hidden_bid_count', 0)} "
                f"({extended_hidden.get('hidden_bid_ratio', 0)*100:.1f}% від BID)"
            )
            report_lines.append(
                f"- Кількість прихованих ASK ордерів: {extended_hidden.get('hidden_ask_count', 0)} "
                f"({extended_hidden.get('hidden_ask_ratio', 0)*100:.1f}% від ASK)"
            )

        # Детальний аналіз spoofing
        detailed_spoofing = order_book_data.get("detailed_spoofing", {})
        if isinstance(detailed_spoofing, dict) and detailed_spoofing:
            report_lines.append("\n**Детальний аналіз spoofing та маніпуляцій:**")
            report_lines.append(
                f"- Стандартне відхилення змін BID: {detailed_spoofing.get('bid_change_std', 0):.2f}"
            )
            report_lines.append(
                f"- Стандартне відхилення змін ASK: {detailed_spoofing.get('ask_change_std', 0):.2f}"
            )

        # Рухи маркетмейкерів/крупних гравців
        market_maker = order_book_data.get("market_maker_moves", {})
        if market_maker.get("detected", False):
            details = market_maker.get("details", [])
            report_lines.append("\n**Рухи маркетмейкерів та крупних гравців:**")
            report_lines.append(f"- Виявлено значущі рухи: {details}")
        else:
            report_lines.append(
                "\n**Рухи маркетмейкерів:** Не виявлено значущих рухів."
            )

        # Поточний стан ринку
        current_state = order_book_data.get("current_state", "Невідомо")
        report_lines.append("\n**Стан ринку:**")
        report_lines.append(f"- {current_state}")

        # Аналіз появи/зникнення великих ордерів
        large_orders_appeared = order_book_data.get("large_orders_appeared", [])
        large_orders_disappeared = order_book_data.get("large_orders_disappeared", [])
        if large_orders_appeared:
            report_lines.append(
                f"\n- Поява нових великих ордерів: {large_orders_appeared}"
            )
        else:
            report_lines.append("\n- Немає появи нових великих ордерів.")
        if large_orders_disappeared:
            report_lines.append(
                f"- Зникнення великих ордерів: {large_orders_disappeared}"
            )
        else:
            report_lines.append("- Немає зникнення великих ордерів.")

        # Рухи капіталу
        report_lines.append("\n**Глибокий аналіз рухів капіталу:**")
        movements = capital_movements.get("potential_movements", [])
        if movements:
            report_lines.append(f"- Виявлено {len(movements)} значущих рухів:")
            for move in movements[:3]:
                ts = move.get("timestamp")
                move_price = move.get("close", 0)
                move_volume = move.get("volume", 0)
                date_str = (
                    ts.strftime("%Y-%m-%d") if hasattr(ts, "strftime") else str(ts)
                )
                report_lines.append(
                    f"  * {date_str}: Ціна {move_price:,.2f}, Обсяг {move_volume:,.2f}"
                )
        else:
            report_lines.append("- Значущі рухи капіталу не виявлено.")

        # Поточний стан активу
        # Обчислюємо поточну ціну як середнє значення зважених цін BID та ASK, якщо вони доступні
        if bid_wap and ask_wap:
            current_asset_price = (bid_wap + ask_wap) / 2
        else:
            current_asset_price = current_price
        report_lines.append("\n**Поточна картина активу:**")
        report_lines.append(f"- Поточна ціна: {current_asset_price:,.2f} USDT")
        report_lines.append(f"- VWAP: {vwap:,.2f} USDT")
        report_lines.append(f"- ATR (волатильність): {atr:,.2f} USDT")

        # Сигнали індикаторів
        report_lines.append("\n**Сигнали індикаторів:**")
        stoch_state = stochastic_signals.get("stoch_signal", "нейтральний")
        rsi_state = rsi_signals.get("rsi_signal", "нейтральний")
        report_lines.append(f"- Stochastic: {stoch_state.capitalize()}")
        report_lines.append(f"- RSI: {rsi_state.capitalize()}")
        if rsi_state.lower() == "overbought":
            report_lines.append(f"  Очікується корекція до підтримки ({bid_wap:,.2f}).")
        elif rsi_state.lower() == "oversold":
            report_lines.append("  Можливий відкат до зростання ціни.")

        # Рекомендації
        report_lines.append("\n**Рекомендації:**")
        report_lines.append(
            "- Для купівлі: входити на рівнях підтримки (біля зваженої ціни BID/VWAP)."
        )
        report_lines.append(f"  Рівень входу: приблизно {bid_wap:,.2f}")
        report_lines.append(f"  Стоп-лосс: нижче за {bid_wap - atr * 0.5:,.2f}")
        report_lines.append(
            "- Для продажу: входити при пробитті опору (біля зваженої ціни ASK)."
        )
        report_lines.append(f"  Рівень входу: приблизно {ask_wap:,.2f}")
        report_lines.append(f"  Стоп-лосс: вище за {ask_wap + atr * 0.5:,.2f}")

        if deep_analysis:
            report_lines.append("\n**Розширений аналіз:**")
            report_lines.append(
                "- Додатковий аналіз патернів ринку та рухів капіталу проводиться в режимі реального часу."
            )

        return "\n".join(report_lines)
