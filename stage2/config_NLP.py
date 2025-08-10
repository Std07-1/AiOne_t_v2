# stage2\config_NLP.py

# -*- coding: utf-8 -*-

"""
Модуль для налаштування NLP для аналізу фінансових даних
"""

# --- SCENARIO_MAP: мова -> стиль -> сценарій ---
SCENARIO_MAP = {
    "UA": {
        "short": {
            "BULLISH_BREAKOUT": "{symbol}: бичий пробій",
            "BEARISH_REVERSAL": "{symbol}: ризик розвороту",
            "RANGE_BOUND": "{symbol}: флет (невизначеність)",
            "BULLISH_CONTROL": "{symbol}: бичий контроль",
            "BEARISH_CONTROL": "{symbol}: ведмежий контроль",
            "HIGH_VOLATILITY": "{symbol}: висока волатильність",
            "MANIPULATED": "{symbol}: можлива маніпуляція",
            "DEFAULT": "{symbol}: ситуація невідома",
        },
        "explain": {
            "BULLISH_BREAKOUT": "{symbol} демонструє потенціал до бичого пробою. Зростаючий обсяг і сила тренду підтримують рух вгору.",
            "BEARISH_REVERSAL": "{symbol} показує ознаки ведмежого розвороту. Слідкуйте за ймовірністю пробою підтримки.",
            "RANGE_BOUND": "{symbol} торгується у боковому діапазоні. Відсутні чіткі трендові сигнали.",
            "BULLISH_CONTROL": "{symbol} знаходиться вище VWAP і RSI > 55, що вказує на бичий контроль.",
            "BEARISH_CONTROL": "{symbol} знаходиться нижче VWAP і RSI < 45, що вказує на ведмежий контроль.",
            "HIGH_VOLATILITY": "{symbol} демонструє високу волатильність (ATR > 3% ціни) - ризик різких рухів.",
            "MANIPULATED": "{symbol} має ознаки маніпуляції, що може вплинути на точність прогнозів.",
            "DEFAULT": "{symbol}: сценарій не визначено.",
        },
        "pro": {
            "BULLISH_BREAKOUT": "{symbol}: інтенсивність зростання підкріплена обсягом, волатильність підвищується, потенціал для пробою над опором.",
            "BEARISH_REVERSAL": "{symbol}: накопичення слабкості, ймовірний злам тренду, індикатори підтверджують фазу корекції.",
            "RANGE_BOUND": "{symbol}: класичний флет із низькою волатильністю (невизначеність), очікуйте реакцію лише на межах діапазону.",
            "BULLISH_CONTROL": "{symbol}: ціна стабільно вище VWAP, індикатори підтверджують бичий контроль, можливі довгі позиції.",
            "BEARISH_CONTROL": "{symbol}: ціна стабільно нижче VWAP, індикатори підтверджують ведмежий контроль, можливі короткі позиції.",
            "HIGH_VOLATILITY": "{symbol}: ринок демонструє високу волатильність, що може призвести до різких рухів. Будьте обережні з позиціями.",
            "MANIPULATED": "{symbol}: виявлено ознаки маніпуляції, що може спотворити ринкові сигнали. Рекомендується обережність.",
            "DEFAULT": "{symbol}: невизначена фаза ринку.",
        },
    },
    "EN": {
        "short": {
            "BULLISH_BREAKOUT": "{symbol}: bullish breakout",
            "BEARISH_REVERSAL": "{symbol}: reversal risk",
            "RANGE_BOUND": "{symbol}: sideways",
            "BULLISH_CONTROL": "{symbol}: bullish control",
            "BEARISH_CONTROL": "{symbol}: bearish control",
            "HIGH_VOLATILITY": "{symbol}: high volatility",
            "MANIPULATED": "{symbol}: manipulated",
            "DEFAULT": "{symbol}: unknown context",
        },
        "explain": {
            "BULLISH_BREAKOUT": "{symbol} shows potential for a bullish breakout. Rising volume and trend strength support upward movement.",
            "BEARISH_REVERSAL": "{symbol} signals possible bearish reversal. Watch for support level breakdown.",
            "RANGE_BOUND": "{symbol} is trading in a sideways range. No strong trend signals.",
            "BULLISH_CONTROL": "{symbol} is above VWAP and RSI > 55, indicating bullish control.",
            "BEARISH_CONTROL": "{symbol} is below VWAP and RSI < 45, indicating bearish control.",
            "HIGH_VOLATILITY": "{symbol} is experiencing high volatility (ATR > 3% of price).",
            "MANIPULATED": "{symbol} shows signs of manipulation, which may affect forecast accuracy.",
            "DEFAULT": "{symbol}: scenario not detected.",
        },
        "pro": {
            "BULLISH_BREAKOUT": "{symbol}: momentum builds, volume confirms move, volatility up, potential breakout above resistance.",
            "BEARISH_REVERSAL": "{symbol}: distribution phase, weakening price action, risk of trend break, confirmed by indicators.",
            "RANGE_BOUND": "{symbol}: classic range, low volatility, valid trades only near boundaries.",
            "BULLISH_CONTROL": "{symbol}: price consistently above VWAP, indicators confirm bullish control, long positions possible.",
            "BEARISH_CONTROL": "{symbol}: price consistently below VWAP, indicators confirm bearish control, short positions possible.",
            "HIGH_VOLATILITY": "{symbol}: market shows high volatility, which may lead to sharp movements. Exercise caution with positions.",
            "MANIPULATED": "{symbol}: signs of manipulation detected, which may distort market signals. Caution is advised.",
            "DEFAULT": "{symbol}: uncertain market phase.",
        },
    },
}

# --- NARRATIVE_BLOCKS: мова -> стиль -> ключ ---
NARRATIVE_BLOCKS = {
    "UA": {
        "short": {
            "LEVEL_PROXIMITY": "Ціна {current_price:.4f} {dist:.2f}% від {level_type} {level:.4f}",
            "SUPPORT_BOUNCE": "Можливий відскок від підтримки.",
            "RESISTANCE_REJECT": "Можливий відбій від опору.",
            "NO_KEY_LEVELS": "Рівні далеко, волатильність підвищена.",
            "TIGHT_RANGE": "Дуже вузький діапазон — очікуйте різких рухів",
            "WIDE_RANGE": "Широкий діапазон — потенціал для довгих утримань",
            "CONSOLIDATION": "Консолідація триває {days} днів",
            "RANGE_NARROWING": "Діапазон звужується — ймовірний пробій",
            "HIGH_BREAKOUT_PROB": "▲ Висока ймовірність пробою (>70%)",
            "LOW_BREAKOUT_PROB": "▼ Низька ймовірність пробою (<30%)",
        },
        "explain": {
            "LEVEL_PROXIMITY": "Поточна ціна {current_price:.4f} знаходиться {dist:.2f}% від {level_type} {level:.4f}.",
            "SUPPORT_BOUNCE": "Ціна близько до підтримки — підвищений шанс на відскок. Слідкуйте за підтвердженням розвороту.",
            "RESISTANCE_REJECT": "Ціна під опором — ймовірний розворот вниз. Варто фіксувати прибуток.",
            "NO_KEY_LEVELS": "Ключові рівні далекі. Можливі несподівані імпульси, варто зменшити розмір позиції.",
            "TIGHT_RANGE": "Дуже вузький діапазон — будьте готові до раптових рухів.",
            "WIDE_RANGE": "Діапазон широкий — можливі затяжні тренди або флет.",
            "CONSOLIDATION": "Консолідація триває {days} днів.",
            "RANGE_NARROWING": "Діапазон звужується — breakout імовірний.",
            "HIGH_BREAKOUT_PROB": "▲ Висока ймовірність пробою (>70%)",
            "LOW_BREAKOUT_PROB": "▼ Низька ймовірність пробою (<30%)",
        },
        "pro": {
            "LEVEL_PROXIMITY": "Ціна {current_price:.4f} на відстані {dist:.2f}% від {level_type} {level:.4f}, активність кластерів на цьому вузлі.",
            "SUPPORT_BOUNCE": "Ліквідність біля підтримки — можливе поглинання і поштовх вгору.",
            "RESISTANCE_REJECT": "Order flow біля опору вказує на можливу відмову.",
            "NO_KEY_LEVELS": "Немає активних рівнів, ймовірна зміна режиму або висока волатильність. Контролюй розмір позиції.",
            "TIGHT_RANGE": "Торгівля у вузькому коридорі — шукайте імпульсний вихід.",
            "WIDE_RANGE": "Ринок дозволяє будувати позиції з розширеним TP/SL.",
            "CONSOLIDATION": "Фаза консолідації затягнулась ({days} днів), підвищений ризик неочікуваного руху.",
            "RANGE_NARROWING": "Звуження флету — breakout зростає.",
            "HIGH_BREAKOUT_PROB": "▲ Ймовірність пробою надзвичайно висока.",
            "LOW_BREAKOUT_PROB": "▼ Ймовірність пробою мінімальна — можлива зупинка чи розворот.",
        },
    },
    "EN": {
        "short": {
            "LEVEL_PROXIMITY": "Price {current_price:.4f} is {dist:.2f}% from {level_type} {level:.4f}",
            "SUPPORT_BOUNCE": "Possible bounce off support.",
            "RESISTANCE_REJECT": "Possible rejection from resistance.",
            "NO_KEY_LEVELS": "Levels far, volatility increased.",
            "TIGHT_RANGE": "Very tight range - expect sharp moves",
            "WIDE_RANGE": "Wide range - potential for extended holds",
            "CONSOLIDATION": "Consolidation ongoing for {days} days",
            "RANGE_NARROWING": "Range narrowing - breakout likely",
            "HIGH_BREAKOUT_PROB": "▲ High breakout probability (>70%)",
            "LOW_BREAKOUT_PROB": "▼ Low breakout probability (<30%)",
        },
        "explain": {
            "LEVEL_PROXIMITY": "Current price {current_price:.4f} is {dist:.2f}% from {level_type} {level:.4f}.",
            "SUPPORT_BOUNCE": "Price is close to support — higher chance for a bounce. Watch for reversal confirmation.",
            "RESISTANCE_REJECT": "Price is near resistance — possible downward reversal. Consider profit-taking.",
            "NO_KEY_LEVELS": "Key levels are distant. Unexpected spikes possible, reduce position size.",
            "TIGHT_RANGE": "Very tight range — be ready for sudden moves.",
            "WIDE_RANGE": "Wide range — allows for extended trends or sideways action.",
            "CONSOLIDATION": "Consolidation ongoing for {days} days.",
            "RANGE_NARROWING": "Range narrowing — breakout risk increasing.",
            "HIGH_BREAKOUT_PROB": "▲ High breakout probability (>70%)",
            "LOW_BREAKOUT_PROB": "▼ Low breakout probability (<30%)",
        },
        "pro": {
            "LEVEL_PROXIMITY": "Price {current_price:.4f} is {dist:.2f}% from {level_type} {level:.4f}, cluster activity present.",
            "SUPPORT_BOUNCE": "Liquidity at support, absorption likely, upward movement expected.",
            "RESISTANCE_REJECT": "Order flow at resistance signals rejection.",
            "NO_KEY_LEVELS": "No active levels, possible regime change/high volatility. Adjust position.",
            "TIGHT_RANGE": "Trading within a tight corridor — watch for impulsive breaks.",
            "WIDE_RANGE": "Market supports wide TP/SL positioning.",
            "CONSOLIDATION": "Extended consolidation ({days} days) — risk of sudden move.",
            "RANGE_NARROWING": "Narrowing range — breakout odds increasing.",
            "HIGH_BREAKOUT_PROB": "▲ Extremely high breakout probability.",
            "LOW_BREAKOUT_PROB": "▼ Low breakout probability — stall or reversal possible.",
        },
    },
}

# --- ANOMALY_MAP: мова -> ключ аномалії -> текст ---
ANOMALY_MAP = {
    "UA": {
        "suspected_manipulation": "⚠️ Потенційні ознаки маніпуляції обсягами",
        "liquidity_issues": "⚠️ Низька ліквідність може підсилити волатильність",
        "unusual_volume_pattern": "⚠️ Незвичний патерн обсягів",
        "flash_crash": "⚠️ Раптовий обвал ціни",
        "volatility_spike": "⚠️ Різке зростання волатильності",
        "high_correlation": "⚠️ Висока кореляція з ринковим індексом",
    },
    "EN": {
        "suspected_manipulation": "⚠️ Possible signs of volume manipulation",
        "liquidity_issues": "⚠️ Low liquidity may increase volatility",
        "unusual_volume_pattern": "⚠️ Unusual volume pattern detected",
        "flash_crash": "⚠️ Sudden price crash",
        "volatility_spike": "⚠️ Volatility spike",
        "high_correlation": "⚠️ High correlation with market index",
    },
}

# --- CLUSTER_TEXT: мова -> стиль -> шаблон ---
CLUSTER_TEXT = {
    "UA": {
        "short": "📊 Кластери: +{pos}/{neg}",
        "explain": "📊 Кластерний аналіз: {conf:.1f}% позитивних ({pos}/{total})",
        "pro": "📊 Кластерна структура: {conf:.1f}% позитивних ({pos}/{total}). Ключові: {drivers}",
    },
    "EN": {
        "short": "📊 Clusters: +{pos}/{neg}",
        "explain": "📊 Cluster analysis: {conf:.1f}% positive ({pos}/{total})",
        "pro": "📊 Cluster structure: {conf:.1f}% positive ({pos}/{total}). Key drivers: {drivers}",
    },
}

# --- LEVEL_TYPE_NAMES: мова -> тип рівня -> напрям -> текст ---
LEVEL_TYPE_NAMES = {
    "UA": {
        "global": {
            "support": "глобальної підтримки",
            "resistance": "глобального опору",
        },
        "local": {"support": "локальної підтримки", "resistance": "локального опору"},
        "cluster": {
            "support": "кластерної підтримки",
            "resistance": "кластерного опору",
        },
        "psych": {
            "support": "психологічної підтримки",
            "resistance": "психологічного опору",
        },
    },
    "EN": {
        "global": {"support": "global support", "resistance": "global resistance"},
        "local": {"support": "local support", "resistance": "local resistance"},
        "cluster": {"support": "cluster support", "resistance": "cluster resistance"},
        "psych": {
            "support": "psychological support",
            "resistance": "psychological resistance",
        },
    },
}

# --- TRIGGER_NAMES: ключ -> текст ---
TRIGGER_NAMES = {
    "volume_spike": "сплеск обсягів",
    "rsi_oversold": "перепроданість RSI",
    "breakout_up": "пробій вгору",
    "vwap_deviation": "відхилення від VWAP",
    "ma_crossover": "перетин ковзних середніх",
    "volume_divergence": "розбіжність обсягів",
    "key_level_break": "пробиття ключового рівня",
    "liquidity_gap": "ліквідні розриви",
    "price_anomaly": "аномалія ціни",
    "volatility_burst": "сплеск волатильності",
    # ...додавай ще, якщо треба...
}
# --- END OF CONFIGURATION ---
