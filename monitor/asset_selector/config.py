"""
monitor/asset_selector/config.py

Конфігураційний модуль з пороговими значеннями для відбору активів.
"""
# Мінімальний середньоденний обсяг торгів (USD)
MIN_AVG_VOLUME = 500_000
# Мінімальна волатильність (логарифмічні доходи)
MIN_VOLATILITY = 0.005
# Мінімальна зміна ціни для вираженого тренду
MIN_PRICE_CHANGE = 0.01
# Мінімальний показник впорядкованості руху ціни
MIN_STRUCTURE_SCORE = 0.7
# Мінімальна відносна сила активу порівняно з ринком
MIN_REL_STRENGTH = 0.5
# Мінімальний Open Interest (якщо застосовується)
MIN_OPEN_INTEREST = 100_000
# Мінімальний поріг відносної волатильності
MIN_RELATIVE_VOLATILITY = 0.8
# Допустимий поріг кореляції з індексом
CORRELATION_THRESHOLD = 0.7
#Open Interest Screener
MIN_VOLUME_ANOMALY = 1.0
