"""
core.py: базові типи, утиліти, базові класи
"""

import logging
import optuna
import warnings

logger = logging.getLogger("calibration_module")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.propagate = False

from optuna.exceptions import ExperimentalWarning

# Ігнорувати попередження про multivariate
warnings.filterwarnings("ignore", category=ExperimentalWarning)

from optuna.logging import set_verbosity

# Вимкнути всі логи Optuna
set_verbosity(optuna.logging.WARNING)
optuna.logging.set_verbosity(optuna.logging.WARNING)
# Або ще сильніше - вимкнути повністю
# optuna.logging.disable_default_handler()
