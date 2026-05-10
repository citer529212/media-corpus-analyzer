from __future__ import annotations

from typing import Optional


def get_empirical_level_ru(level: str) -> str:
    mp = {
        "very_low": "очень низкий",
        "low": "низкий",
        "medium": "средний",
        "elevated": "повышенный",
        "high": "высокий",
        "extreme": "экстремально высокий",
    }
    return mp.get(level, level)


def interpret_indicator(index_name: str, raw_value: float, percentile: float, empirical_level: str, calibration_type: Optional[str] = None, language: str = "en") -> str:
    p = int(round(float(percentile)))
    if language.lower().startswith("ru"):
        tail = f" (тип baseline: {calibration_type})" if calibration_type else ""
        return (
            f"Значение {index_name} = {raw_value:.4f} соответствует {p}-му процентилю калибровочного корпуса. "
            f"Это {get_empirical_level_ru(empirical_level)} уровень относительно эмпирической нормы{tail}."
        )
    tail = f" (baseline type: {calibration_type})" if calibration_type else ""
    return (
        f"{index_name} = {raw_value:.4f} corresponds to the {p}th percentile of the calibration corpus. "
        f"This indicates {empirical_level} level relative to the empirical baseline{tail}."
    )
