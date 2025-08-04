# stage2\config_NLP.py

# Модуль для налаштування NLP для аналізу фінансових даних
# -*- coding: utf-8 -*-

# Конфігурація NLP
SCENARIO_MAP = {
    "BULLISH_BREAKOUT": "{symbol} демонструє потенціал до бичого пробою",
    "BEARISH_REVERSAL": "{symbol} показує ризики ведмежого розвороту",
    "HIGH_VOLATILITY": "{symbol} у фазі підвищеної волатильності",
    "BULLISH_CONTROL": "{symbol} під контролем покупців",
    "BEARISH_CONTROL": "{symbol} під контролем продавців",
    "RANGE_BOUND": "{symbol} торгується в боковому діапазоні",
    "DEFAULT": "Невизначений сценарій для {symbol}",
}

# Імена тригерів для NLP
TRIGGER_NAMES = {
    "volume_spike": "сплеск обсягів",
    "rsi_oversold": "перепроданість RSI",
    "breakout_up": "пробій вгору",
    "vwap_deviation": "відхилення від VWAP",
    "ma_crossover": "перетин ковзних середніх",
    "volume_divergence": "розбіжність обсягів",
    "key_level_break": "пробиття ключового рівня",
}
