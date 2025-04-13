import os
import logging
import json
import datetime as dt
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from statsmodels.tsa.arima.model import ARIMA
from typing import Dict, List, Optional

logger = logging.getLogger("adaptive_thresholds")
logger.setLevel(logging.INFO)


class AdaptiveThresholds:
    def __init__(
        self,
        asset: str,
        storage_path: str = "thresholds_data",
        base_window: int = 14,          # додано base_window
        volatility_multiplier: float = 1.5,
        regime_clusters: int = 3,         # додано regime_clusters
        update_interval: int = 7,
        shock_sensitivity: float = 0.25
    ):
        """
        Ініціалізація адаптивних порогів для активу.

        :param asset: Назва активу (наприклад, BTCUSDT).
        :param storage_path: Шлях для збереження файлу порогів.
        :param update_interval: Період (у днях) для оновлення глобальних порогів.
        :param shock_sensitivity: Коефіцієнт, після якого зміни вважаються аномальними.
        """
        self.asset = asset
        self.storage_path = storage_path
        self.base_window = base_window            # зберігаємо параметр
        self.volatility_multiplier = volatility_multiplier
        self.regime_clusters = regime_clusters      # зберігаємо параметр
        self.update_interval = update_interval
        self.shock_sensitivity = shock_sensitivity

        self.file_path = os.path.join(storage_path, f"{asset}_thresholds.json")
        self.ensure_storage_path()
        # Завантаження історичних даних порогів
        self.thresholds = self.load_thresholds()
        self.current_regime = None

    def ensure_storage_path(self):
        """Створює директорію для збереження даних, якщо вона не існує."""
        if not os.path.exists(self.storage_path):
            os.makedirs(self.storage_path)
            logger.info(f"📂 Створено директорію {self.storage_path}")

    def load_thresholds(self) -> dict:
        """
        Завантажує пороги з файлу або створює нову структуру за замовчуванням.

        :return: Словник з полями:
            {
                "global_thresholds": {},
                "short_term_thresholds": {},
                "last_update_day": 0,
                "history": []
            }
        """
        if os.path.exists(self.file_path):
            try:
                with open(self.file_path, "r") as f:
                    return json.load(f)
            except json.JSONDecodeError as e:
                logger.error(f"Помилка декодування JSON: {e}")
        logger.warning(f"⚠️ Пороги для {self.asset} не знайдено. Використовується порожній набір.")
        return {
            "global_thresholds": {},
            "short_term_thresholds": {},
            "last_update_day": 0,
            "history": []
        }

    def save_thresholds(self):
        """Зберігає поточні пороги у файл у форматі JSON з відступами."""
        with open(self.file_path, "w") as f:
            json.dump(self.thresholds, f, indent=4)
        logger.info(f"💾 Пороги для {self.asset} збережено -> {self.file_path}")

    def calculate_dynamic_thresholds(self, df: pd.DataFrame) -> Dict:
        """
        Розраховує адаптивні пороги на основі ковзного вікна та базових статистичних методів.
        Використовує дані за останній період (base_window) для визначення середніх значень,
        стандартного відхилення, тощо.

        :param df: DataFrame з необхідними колонками ('volatility', 'volume', 'price_change').
        :return: Словник із порогами.
        """
        # Якщо стовпець 'price_change' відсутній, створюємо його
        if "price_change" not in df.columns:
            df["price_change"] = df["close"].pct_change()
            logger.debug("Стовпець 'price_change' створено за допомогою pct_change().")
            
        window_size = min(self.update_interval, len(df))
        rolling_data = df.iloc[-window_size:]

        thresholds = {
            'volatility_median': float(rolling_data['volatility'].median()),
            'volatility_threshold': float(rolling_data['volatility'].median() * 1.2),
            'volume_threshold': float(rolling_data['volume'].quantile(0.8)),
            'avg_volume': float(rolling_data['volume'].mean()),
            'weekly_volume': float(rolling_data['volume'].rolling(7).mean().iloc[-1] if len(df) >= 7 else rolling_data['volume'].mean())
        }

        if 'rsi' in df.columns:
            rsi_vals = df['rsi'].dropna()
            if not rsi_vals.empty:
                thresholds.update({
                    'rsi_lower': float(rsi_vals.quantile(0.25)),
                    'rsi_upper': float(rsi_vals.quantile(0.75))
                })

        # Виявлення ринкових режимів
        self.identify_market_regimes(df)
        # Прогнозування наступного періоду
        thresholds['forecast'] = self.predict_next_period(df)
        return thresholds

    def identify_market_regimes(self, df: pd.DataFrame):
        """
        Використовує K-means для кластеризації ринкових режимів за характеристиками:
        волатильність, обсяг і зміна ціни.
        """
        features = df[['volatility', 'volume', 'price_change']].dropna()
        if len(features) >= 3:
            kmeans = KMeans(n_clusters=3)
            clusters = kmeans.fit_predict(features)
            self.current_regime = int(clusters[-1])
            self.thresholds['regimes'] = clusters.tolist()

    def predict_next_period(self, df: pd.DataFrame) -> Dict:
        """
        Прогнозує значення ключових показників на наступний період за допомогою ARIMA.
        Використовує історичні дані з self.thresholds['history'] для кожного параметра.

        :param df: DataFrame з необхідними колонками ('volatility', 'volume', 'price_change').
        :return: Словник із прогнозованими значеннями для параметрів,
                 наприклад, {'volatility_median': 0.061, 'volume_threshold': 1300000.0, ...}
        """
        forecasts = {}
        # Параметри, для яких будемо прогнозувати наступне значення
        params = ['volatility_median', 'volume_threshold', 'avg_volume', 'weekly_volume']
        # Отримуємо історичні дані з записів у self.thresholds['history']
        history = [entry["changes"] for entry in self.thresholds.get("history", [])]
        if not history:
            return self.thresholds.get("global_thresholds", {})

        for param in params:
            values = [entry[param] for entry in history if param in entry]
            if len(values) >= 5:
                try:
                    model = ARIMA(values, order=(1, 1, 1))
                    model_fit = model.fit()
                    forecast = float(model_fit.forecast(steps=1).iloc[0])
                    forecasts[param] = forecast
                except Exception as e:
                    logger.error(f"Помилка прогнозування для {param}: {e}")
                    forecasts[param] = None
            else:
                forecasts[param] = None
        return forecasts

    def detect_anomalies(self, current: Dict, historical: List[Dict]) -> Dict:
        """
        Виявляє аномалії у поточних порогах, використовуючи Z-скори,
        порівнюючи з історичними значеннями.

        :param current: Нові порогові значення.
        :param historical: Список історичних записів (словників з порогами).
        :return: Словник з виявленими аномаліями.
        """
        anomalies = {}
        for key in ['volatility', 'volume', 'price_change']:
            values = [entry[key] for entry in historical if key in entry]
            if len(values) > 10:
                mean_val = np.mean(values)
                std_val = np.std(values)
                if std_val > 0:
                    z_score = (current[key] - mean_val) / std_val
                    if abs(z_score) > self.shock_sensitivity:
                        anomalies[key] = {
                            'value': current[key],
                            'z_score': z_score,
                            'mean': mean_val,
                            'std': std_val
                        }
        return anomalies

    def adjust_for_anomalies(self, thresholds: Dict, anomalies: Dict) -> Dict:
        """
        Плавна корекція порогів за допомогою вагових коефіцієнтів,
        якщо виявлено аномалії.
        """
        adjusted = thresholds.copy()
        for key in anomalies:
            # Плавне згладжування з коефіцієнтом 0.2
            adjusted[key] = 0.8 * thresholds[key] + 0.2 * anomalies[key]['mean']
        return adjusted

    def update_prediction_model(self, new_data: pd.DataFrame):
        """
        Можливе перетренування моделі прогнозування на основі нових даних.
        """
        # Реалізуйте логіку оновлення моделі при потребі
        pass

    def update_thresholds(self, new_data: pd.DataFrame, current_day: int) -> Dict:
        """
        Оновлює пороги, враховуючи нові дані та історичну динаміку.
        
        :param new_data: DataFrame з новими даними.
        :param current_day: Поточний день (числове значення).
        :return: Нові порогові значення після корекції.
        """
        new_thresholds = self.calculate_dynamic_thresholds(new_data)
        history_entry = {
            "date": str(dt.date.today()),
            "changes": new_thresholds
        }

        # Отримуємо історію змін
        historical = [entry["changes"] for entry in self.thresholds.get("history", [])]
        anomalies = self.detect_anomalies(new_thresholds, historical)
        if anomalies:
            logger.warning(f"Виявлено аномалії: {list(anomalies.keys())}")
            new_thresholds = self.adjust_for_anomalies(new_thresholds, anomalies)

        # Оновлення короткострокових порогів
        self.thresholds.setdefault("short_term_thresholds", {}).update(new_thresholds)
        last_update_day = self.thresholds.get("last_update_day", 0)
        if (current_day - last_update_day) >= self.update_interval:
            logger.info(f"🔄 Оновлення глобальних порогів для {self.asset}")
            self.thresholds["global_thresholds"] = self.thresholds["short_term_thresholds"].copy()
            self.thresholds["last_update_day"] = current_day

        self.thresholds.setdefault("history", []).append(history_entry)
        self.save_thresholds()
        return new_thresholds

    def analyze_historical_trends(self, days_back: int = 30) -> Dict[str, Dict[str, float]]:
        """
        Аналізує історичні дані змін порогів за останні 'days_back' днів за допомогою
        лінійної регресії (np.polyfit) та повертає тенденції для кожного параметра.

        :param days_back: Кількість днів для аналізу.
        :return: Словник з тенденціями для кожного параметра,
                 наприклад: {'volatility_threshold': {'slope': 0.001, 'intercept': 0.04}, ...}
        """
        history = self.thresholds.get("history", [])
        trends = {}
        filtered_history = [
            entry for entry in history
            if (dt.date.today() - dt.date.fromisoformat(entry["date"])).days <= days_back
        ]
        if not filtered_history:
            logger.info(f"За останні {days_back} днів історія змін порогів відсутня.")
            return trends
        
        # Для кожного параметра, який нас цікавить
        parameters = ['volatility_median', 'volatility_threshold', 'volume_threshold', 'avg_volume', 'weekly_volume']
        for param in parameters:
            # Формуємо списки дат і значень
            dates = []
            values = []
            for entry in filtered_history:
                date_obj = dt.date.fromisoformat(entry["date"])
                if param in entry["changes"]:
                    dates.append(date_obj.toordinal())  # перетворюємо дату в числовий формат
                    values.append(entry["changes"][param])
            if len(dates) >= 2:
                try:
                    # Використовуємо np.polyfit для лінійної регресії
                    slope, intercept = np.polyfit(dates, values, 1)
                    trends[param] = {"slope": slope, "intercept": intercept}
                except Exception as e:
                    logger.error(f"Помилка аналізу тенденцій для {param}: {e}")
        return trends

    def get_adaptive_thresholds(self, df: pd.DataFrame, current_day: int) -> dict:
        """
        Перевіряє, чи потрібне оновлення порогів (за розкладом або через відсутність збережених даних),
        виконує оновлення через update_thresholds та повертає актуальні global_thresholds.
        
        :param df: DataFrame з необхідними даними.
        :param current_day: Поточний день.
        :return: Актуальні глобальні пороги.
        """
        saved_thresholds = self.thresholds.get("global_thresholds", {})
        last_update_day = self.thresholds.get("last_update_day", 0)
        need_recalculate = (current_day - last_update_day) >= self.update_interval or not saved_thresholds
        if need_recalculate:
            logger.info(f"[{self.asset}] Виконується оновлення порогів.")
            new_thresholds = {
                "volatility_median": float(df["volatility"].median()),
                "volatility_threshold": float(df["volatility"].median() * 1.2),
                "volume_threshold": float(df["volume"].quantile(0.8)),
                "avg_volume": float(df["volume"].mean()),
                "weekly_volume": float(df["volume"].rolling(7).mean().iloc[-1] if len(df) >= 7 else df["volume"].mean())
            }
            self.update_thresholds(new_data=df, current_day=current_day)
        else:
            logger.info(f"[{self.asset}] Використовуємо поточні пороги без оновлення.")
        return self.thresholds.get("global_thresholds", {})

    def generate_report(self, days_back: int = 7) -> str:
        """
        Генерує звіт з історії змін порогів за останні days_back днів.
    
        :param days_back: Кількість днів для аналізу історії.
        :return: Текстовий звіт.
        """
        # Локальна функція для форматування чисел
        def format_number(num, decimals=2):
            try:
                return f"{num:,.{decimals}f}"
            except Exception:
                return str(num)
        
        relevant_history = [
            entry for entry in self.thresholds.get("history", [])
            if (dt.date.today() - dt.date.fromisoformat(entry["date"])).days <= days_back
        ]
        if not relevant_history:
            return f"За останні {days_back} днів змін у порогах для {self.asset} не зафіксовано."
    
        report_lines = [f"📊 Звіт про адаптивні пороги для {self.asset} (останні {len(relevant_history)} днів):"]
        for entry in relevant_history:
            report_lines.append(f"\n📅 Дата: {entry['date']}")
            for key, value in entry["changes"].items():
                report_lines.append(f"  🔹 {key}: {value}")
    
        trends = self.analyze_historical_trends(days_back)
        if trends:
            report_lines.append("\n📈 Тенденції змін порогів:")
            for param, stats in trends.items():
                report_lines.append(
                    f"  {param}: зміна (slope) = {format_number(stats['slope'], 4)}, "
                    f"базове значення (intercept) = {format_number(stats['intercept'], 4)}"
                )
    
        return "\n".join(report_lines)



# =============================================================================
# Приклад використання
# =============================================================================
"""
    # 1) Ініціалізуємо клас порогів
    adaptive_thresholds = AdaptiveThresholds(asset="BTCUSDT")

    # 2) Отримуємо (або, якщо потрібно, розраховуємо й оновлюємо) глобальні пороги
    thresholds = adaptive_thresholds.get_adaptive_thresholds(df, current_day)
    logger.info(f"Теперішні (глобальні) пороги: {thresholds}")

    # 3) Формуємо звіт за останні 30 днів
    report = adaptive_thresholds.generate_report(days_back=30)
    logger.info(f"\n{report}")
"""