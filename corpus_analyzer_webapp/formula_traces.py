from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, asdict
from typing import Dict, List

import pandas as pd


@dataclass
class FormulaTrace:
    trace_id: str
    context_id: str
    formula_name: str
    formula_symbolic: str
    formula_substitution: str
    result_raw: float
    result_percent: float | None
    interpretation_short: str
    inputs_json: str

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)


def _trace_id(prefix: str, context_id: str) -> str:
    return f"{prefix}_{context_id}_{uuid.uuid4().hex[:8]}"


def build_context_formula_traces(row: pd.Series) -> List[FormulaTrace]:
    context_id = str(row.get("context_id", ""))
    n_content = float(row.get("N_content", 0) or 0)
    n_ideol = float(row.get("N_ideol", 0) or 0)
    n_e_w = float(row.get("N_e_w", 0) or 0)
    n_e_m = float(row.get("N_e_m", 0) or 0)
    n_e_s = float(row.get("N_e_s", 0) or 0)
    n_met = float(row.get("N_met", 0) or 0)
    idi = float(row.get("IDI_raw", row.get("IDI", 0.0)) or 0.0)
    emi = float(row.get("EMI_raw", row.get("EMI", 0.0)) or 0.0)
    mti = float(row.get("MTI_raw", row.get("MTI", 0.0)) or 0.0)
    p_score = float(row.get("positive_score", 0) or 0)
    n_score = float(row.get("negative_score", 0) or 0)
    evi = float(row.get("EVI", row.get("EVI_raw", 0)) or 0)
    evi_norm = float(row.get("EVI_norm", 0) or 0)
    ip_i = float(row.get("IP_i", row.get("IP_context", 0)) or 0)
    s_r = float(row.get("S_r", row.get("referent_salience", 0)) or 0)

    traces: List[FormulaTrace] = []
    traces.append(
        FormulaTrace(
            trace_id=_trace_id("IDI", context_id),
            context_id=context_id,
            formula_name="Индекс идеологической маркированности",
            formula_symbolic="IDI_r = N_ideol_r / N_content_r",
            formula_substitution=f"IDI_r = {int(n_ideol)} / {int(n_content)} = {idi:.6f}",
            result_raw=idi,
            result_percent=idi * 100.0,
            interpretation_short=f"Идеологические маркеры составляют {idi * 100.0:.4f}% знаменательной лексики контекста.",
            inputs_json=json.dumps({"N_ideol_r": int(n_ideol), "N_content_r": int(n_content)}, ensure_ascii=False),
        )
    )
    weighted_emotion = (n_e_w / 3.0) + (2.0 * n_e_m / 3.0) + n_e_s
    traces.append(
        FormulaTrace(
            trace_id=_trace_id("EMI", context_id),
            context_id=context_id,
            formula_name="Индекс эмоциональной окрашенности",
            formula_symbolic="EMI_r = (1/3×N_e_w_r + 2/3×N_e_m_r + 1×N_e_s_r) / N_content_r",
            formula_substitution=(
                f"EMI_r = (1/3×{int(n_e_w)} + 2/3×{int(n_e_m)} + 1×{int(n_e_s)}) / {int(n_content)}"
                f" = {weighted_emotion:.6f} / {int(n_content)} = {emi:.6f}"
            ),
            result_raw=emi,
            result_percent=emi * 100.0,
            interpretation_short=f"Эмоциональные маркеры с учетом интенсивности составляют {emi * 100.0:.4f}%.",
            inputs_json=json.dumps(
                {"N_e_w_r": int(n_e_w), "N_e_m_r": int(n_e_m), "N_e_s_r": int(n_e_s), "N_content_r": int(n_content)},
                ensure_ascii=False,
            ),
        )
    )
    traces.append(
        FormulaTrace(
            trace_id=_trace_id("MTI", context_id),
            context_id=context_id,
            formula_name="Индекс метафоричности",
            formula_symbolic="MTI_r = N_met_r / N_content_r",
            formula_substitution=f"MTI_r = {int(n_met)} / {int(n_content)} = {mti:.6f}",
            result_raw=mti,
            result_percent=mti * 100.0,
            interpretation_short=f"Метафорические единицы составляют {mti * 100.0:.4f}% знаменательной лексики.",
            inputs_json=json.dumps({"N_met_r": int(n_met), "N_content_r": int(n_content)}, ensure_ascii=False),
        )
    )
    traces.append(
        FormulaTrace(
            trace_id=_trace_id("EVI", context_id),
            context_id=context_id,
            formula_name="Оценочный вектор",
            formula_symbolic="EVI_r = P_r - N_r",
            formula_substitution=f"EVI_r = {p_score:.2f} - {n_score:.2f} = {evi:.2f}",
            result_raw=evi,
            result_percent=None,
            interpretation_short="Положительный EVI означает позитивную репрезентацию, отрицательный — негативную.",
            inputs_json=json.dumps({"P_r": p_score, "N_r": n_score}, ensure_ascii=False),
        )
    )
    traces.append(
        FormulaTrace(
            trace_id=_trace_id("EVI_NORM", context_id),
            context_id=context_id,
            formula_name="Нормированный оценочный вектор",
            formula_symbolic="EVI_norm_r = EVI_r / 10",
            formula_substitution=f"EVI_norm_r = {evi:.2f} / 10 = {evi_norm:.6f}",
            result_raw=evi_norm,
            result_percent=None,
            interpretation_short="EVI_norm задает базовое направление воздействия в диапазоне [-1; +1].",
            inputs_json=json.dumps({"EVI_r": evi}, ensure_ascii=False),
        )
    )
    traces.append(
        FormulaTrace(
            trace_id=_trace_id("IP", context_id),
            context_id=context_id,
            formula_name="Воздействующий потенциал контекста",
            formula_symbolic="IP_i = EVI_norm_i × (1 + IDI_i + EMI_i + MTI_i)",
            formula_substitution=(
                f"IP_i = {evi_norm:.6f} × (1 + {idi:.6f} + {emi:.6f} + {mti:.6f}) = {ip_i:.6f}"
            ),
            result_raw=ip_i,
            result_percent=None,
            interpretation_short="IDI/EMI/MTI усиливают оценочный вектор EVI_norm в контексте.",
            inputs_json=json.dumps(
                {"EVI_norm_i": evi_norm, "IDI_i": idi, "EMI_i": emi, "MTI_i": mti, "S_r": s_r},
                ensure_ascii=False,
            ),
        )
    )
    return traces


def traces_to_dataframe(traces: List[FormulaTrace]) -> pd.DataFrame:
    if not traces:
        return pd.DataFrame(
            columns=[
                "trace_id",
                "context_id",
                "formula_name",
                "formula_symbolic",
                "formula_substitution",
                "result_raw",
                "result_percent",
                "interpretation_short",
                "inputs_json",
            ]
        )
    return pd.DataFrame([t.to_dict() for t in traces])

