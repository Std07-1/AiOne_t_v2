from typing import List, Dict
from collections import defaultdict
import logging

from rich.console import Console
from rich.logging import RichHandler

# --- Налаштування логування ---
logger = logging.getLogger("report_generator")
logger.setLevel(logging.DEBUG)  # Рівень логування DEBUG для детального відстеження
logger.handlers.clear()
logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
logger.propagate = False


class ReportGenerator:
    """
    Клас для генерації зведених та деталізованих звітів за результатами бектестингу.
    """

    def __init__(self, analysis_report: Dict):
        """
        Ініціалізація генератора звітів.
        :param analysis_report: Словник з результатами аналізу
        """
        self.analysis_report = analysis_report
        self.summary = {}
        logger.info("ReportGenerator ініціалізовано")

    def generate_summary(self):
        """
        Генерація зведеної статистики по кожному символу.
        :return: Словник з підсумковими метриками
        """
        logger.info("Генерація зведеної статистики...")
        for symbol, results in self.analysis_report.items():
            total = len(results)
            correct = sum(1 for r in results if r["accuracy"] == "correct")
            accuracy = correct / total * 100 if total > 0 else 0

            scenario_accuracy = {}
            for scenario in set(r["scenario"] for r in results):
                scenario_results = [r for r in results if r["scenario"] == scenario]
                sc_total = len(scenario_results)
                sc_correct = sum(
                    1 for r in scenario_results if r["accuracy"] == "correct"
                )
                scenario_accuracy[scenario] = (
                    sc_correct / sc_total * 100 if sc_total > 0 else 0
                )

            self.summary[symbol] = {
                "total_signals": total,
                "accuracy": accuracy,
                "scenario_accuracy": scenario_accuracy,
                "common_triggers": self.get_common_triggers(results),
            }
            logger.debug(f"Зведена статистика для {symbol}: {self.summary[symbol]}")

        logger.info("Зведена статистика сформована")
        return self.summary

    def get_common_triggers(self, results: List[Dict]) -> Dict:
        """
        Аналіз найпоширеніших тригерів для коректних сигналів.
        :param results: Список результатів по сигналам
        :return: Словник з кількістю спрацювань кожного тригера
        """
        trigger_counts = defaultdict(int)
        correct_results = [r for r in results if r["accuracy"] == "correct"]

        for result in correct_results:
            for trigger in result["triggers"]:
                trigger_counts[trigger] += 1

        logger.debug(f"Найпоширеніші тригери: {dict(trigger_counts)}")
        return dict(sorted(trigger_counts.items(), key=lambda x: x[1], reverse=True))

    def generate_detailed_report(self, output_format: str = "csv"):
        """
        Генерація деталізованого звіту (експорт у CSV/Excel/HTML).
        :param output_format: Формат експорту (csv, excel, html)
        """
        logger.info(f"Генерація деталізованого звіту у форматі: {output_format}")
        # TODO: Реалізувати експорт у відповідний формат
        pass
