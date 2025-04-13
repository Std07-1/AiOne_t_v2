import pandas as pd
import logging
from typing import Any, Dict, Optional, Union, Callable

logger = logging.getLogger("ColumnValidator")


class ColumnValidator:
    """
    Клас для перевірки наявності необхідних колонок у DataFrame.
    """

    def __init__(self, default_value: Union[Any, Dict[str, Any], Callable[[str], Any]] = 0):
        """
        Ініціалізація ColumnValidator.

        Args:
            default_value (Union[Any, Dict[str, Any], Callable[[str], Any]]): Значення за замовчуванням
                для відсутніх колонок. Може бути:
                - єдине значення (наприклад, 0),
                - словник, де ключ — це назва колонки, а значення — значення за замовчуванням,
                - функція, яка повертає значення для колонки.
        """
        self.default_value = default_value

    async def validate(
        self,
        df: Optional[pd.DataFrame],
        required_columns: set,
    ) -> pd.DataFrame:
        """
        Перевіряє, чи DataFrame містить усі необхідні колонки. Додає відсутні колонки зі значенням за замовчуванням.

        Args:
            df (Optional[pd.DataFrame]): Вхідний DataFrame для перевірки.
            required_columns (set): Набір колонок, які повинні бути присутніми.

        Returns:
            pd.DataFrame: Оновлений DataFrame із доданими відсутніми колонками.
        """
        try:
            logger.info("=== [ColumnValidator] Запуск перевірки колонок ===")

            if df is None or df.empty:
                logger.warning("[ColumnValidator] Вхідний DataFrame порожній. Створюємо новий із необхідними колонками.")
                return pd.DataFrame({col: [self._get_default_value(col)] for col in required_columns})

            if not isinstance(df, pd.DataFrame):
                logger.error("[ColumnValidator] Аргумент 'df' не є DataFrame.")
                raise TypeError("Аргумент 'df' повинен бути DataFrame.")

            missing_columns = set(required_columns) - set(df.columns)

            if missing_columns:
                logger.warning(f"[ColumnValidator] Відсутні колонки: {missing_columns}")
                for col in missing_columns:
                    df[col] = self._get_default_value(col)
                    logger.info(f"[ColumnValidator] Додано колонку '{col}' зі значенням {self._get_default_value(col)}.")

            logger.info("[ColumnValidator] Усі необхідні колонки перевірено.")
            return df

        except Exception as e:
            logger.error(f"[ColumnValidator] Помилка: {e}", exc_info=True)
            return pd.DataFrame()

    def _get_default_value(self, column_name: str) -> Any:
        """
        Отримує значення за замовчуванням для колонки.

        Args:
            column_name (str): Назва колонки.

        Returns:
            Any: Значення за замовчуванням.
        """
        if callable(self.default_value):
            return self.default_value(column_name)
        if isinstance(self.default_value, dict):
            return self.default_value.get(column_name, 0)
        return self.default_value
