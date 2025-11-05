# main.py
# Lógica de datos para el dashboard KYC (Compliance 360) – Vista Única (8 puntos)
# --------------------------------------------------------------------------------
# Requisitos: pandas, numpy, openpyxl (para leer .xlsx)

from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple, List, Optional

import numpy as np
import pandas as pd
import datetime

# =============================================================================
# 0) Configuración y utilidades generales
# =============================================================================

# Nombres de hojas por defecto (tolerantes a cambios de mayúsculas/minúsculas)
SHEETS_DEFAULT = {
    "customers": "Customers",
    "kyc": "KYC_Cases",
    "analysts": "Analysts",
}

# Normalización de estados KYC
STATUS_MAP = {
    "complete": "Complete",
    "completed": "Complete",
    "pending": "Pending",
    "under review": "Under_Review",
    "under_review": "Under_Review",
    "under-review": "Under_Review",
    "escalated": "Escalated",
}
PENDING_SET = {"Pending", "Under_Review", "Escalated"}

# Columnas esperadas
# Customers
C_CUSTOMER_ID   = "Customer_ID"
C_RISK_LEVEL    = "Risk_Level"
C_SECTOR        = "Sector"
C_COUNTRY       = "Country"
C_ONBOARD       = "Onboarding_Date"
C_LAST_REVIEW   = "Last_Review_Date"
# KYC
K_CASE_ID       = "KYC_Case_ID"
K_CUSTOMER_ID   = "Customer_ID"
K_STATUS        = "Status"
K_DEP           = "Dependency"
K_ASSIGNED      = "Assigned_Analyst"
K_CREATED       = "Creation_Date"
K_COMPLETED     = "Completion_Date"
K_DUE           = "Due_Date"
K_PT_DAYS       = "Processing_Time_Days"
K_QUALITY       = "Quality_Score"
K_RISK_RATING   = "Risk_Rating"
# Analysts
A_ID            = "Analyst_ID"
A_NAME          = "Analyst_Name"
A_CASES_PM      = "Cases_Per_Month"
A_QUALITY       = "Quality_Score"
A_PERF          = "Performance_Rating"
A_AVG_HOURS     = "Average_Resolution_Time_Hours"


def _coerce_datetime(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df


def _norm_str_series(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
         .str.strip()
         .str.replace(r"[_\-]+", " ", regex=True)
         .str.lower()
    )


def _safe_div(num: float, den: float) -> float:
    return float(num) / float(den) if den not in (0, 0.0, None, np.nan) else np.nan


def _normalize_status(df: pd.DataFrame) -> pd.DataFrame:
    if K_STATUS in df.columns:
        norm = _norm_str_series(df[K_STATUS])
        df[K_STATUS] = norm.map(lambda x: STATUS_MAP.get(x, None))
        # Fallback: “Title Case” + guiones bajos
        df[K_STATUS] = df[K_STATUS].fillna(
            _norm_str_series(df[K_STATUS]).str.title().str.replace(" ", "_")
        )
    return df


def _ensure_processing_days(df: pd.DataFrame) -> pd.DataFrame:
    # Si falta Processing_Time_Days, lo derivamos de Completion - Creation
    if K_PT_DAYS not in df.columns and {K_CREATED, K_COMPLETED}.issubset(df.columns):
        df[K_PT_DAYS] = (df[K_COMPLETED] - df[K_CREATED]).dt.days
    return df


# =============================================================================
# 1) Carga y normalización de datos
# =============================================================================

def load_all(
    path: str | Path = Path("data") / "AML_KPIs_PowerBI_Complete.xlsx",
    sheets: Dict[str, str] = SHEETS_DEFAULT,
) -> Dict[str, pd.DataFrame]:
    """
    Carga el Excel y devuelve:
      {"customers": df, "kyc": df, "analysts": df}
    - Tolera nombres de hoja con variaciones de mayúsculas/minúsculas.
    - Por defecto, usa data/AML_KPIs_PowerBI_Complete.xlsx.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"No se encontró el archivo: {path.resolve()}")

    xls = pd.ExcelFile(path)

    def resolve(sheet_guess: str) -> Optional[str]:
        for real in xls.sheet_names:
            if real.strip().lower() == sheet_guess.strip().lower():
                return real
        return None

    resolved = {k: resolve(v) for k, v in sheets.items()}

    out: Dict[str, pd.DataFrame] = {}
    out["customers"] = pd.read_excel(xls, sheet_name=resolved["customers"]) if resolved.get("customers") else pd.DataFrame()
    out["kyc"]       = pd.read_excel(xls, sheet_name=resolved["kyc"])       if resolved.get("kyc")       else pd.DataFrame()
    out["analysts"]  = pd.read_excel(xls, sheet_name=resolved["analysts"])  if resolved.get("analysts")  else pd.DataFrame()

    return normalize_frames(out)


def normalize_frames(dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Normaliza tipos/fechas y columnas clave; crea Processing_Time_Days si falta;
    homogeneiza Status en KYC; añade Avg_Resolution_Days si procede.
    """
    customers = dfs.get("customers", pd.DataFrame()).copy()
    kyc       = dfs.get("kyc", pd.DataFrame()).copy()
    analysts  = dfs.get("analysts", pd.DataFrame()).copy()

    if not customers.empty:
        customers = _coerce_datetime(customers, [C_ONBOARD, C_LAST_REVIEW])

    if not kyc.empty:
        kyc = _coerce_datetime(kyc, [K_CREATED, K_COMPLETED, K_DUE])
        kyc = _normalize_status(kyc)
        kyc = _ensure_processing_days(kyc)

    # Analysts: convertir horas → días si está disponible
    if not analysts.empty and A_AVG_HOURS in analysts.columns:
        with np.errstate(invalid="ignore", divide="ignore"):
            analysts["Avg_Resolution_Days"] = pd.to_numeric(
                analysts[A_AVG_HOURS], errors="coerce"
            ) / 24.0

    return {"customers": customers, "kyc": kyc, "analysts": analysts}


# =============================================================================
# 2) Filtros (sidebar) y aplicación coherente a las tablas
# =============================================================================

@dataclass
class Filters:
    date_from: Optional[pd.Timestamp] = None
    date_to: Optional[pd.Timestamp] = None
    country: Optional[List[str]] = None
    sector: Optional[List[str]] = None
    risk: Optional[List[str]] = None
    analyst: Optional[List[str]] = None

    def is_active(self) -> bool:
        return any([self.date_from is not None, self.date_to is not None,
                    self.country, self.sector, self.risk, self.analyst])


def build_filters(customers: pd.DataFrame, kyc: pd.DataFrame, analysts: pd.DataFrame) -> Dict[str, List[str]]:
    """
    Devuelve listas únicas para poblar selects: países, sectores, niveles de riesgo, analistas.
    """
    result = {"countries": [], "sectors": [], "risk_levels": [], "analysts": []}

    if not customers.empty:
        if C_COUNTRY in customers.columns:
            result["countries"] = (
                customers[C_COUNTRY].dropna().astype(str).sort_values().unique().tolist()
            )
        if C_SECTOR in customers.columns:
            result["sectors"] = (
                customers[C_SECTOR].dropna().astype(str).sort_values().unique().tolist()
            )
        if C_RISK_LEVEL in customers.columns:
            risk_norm = customers[C_RISK_LEVEL].dropna().astype(str).str.title()
            result["risk_levels"] = sorted(risk_norm.unique().tolist())

    names = set()
    if not analysts.empty and A_NAME in analysts.columns:
        names.update(analysts[A_NAME].dropna().astype(str).tolist())
    if not kyc.empty and K_ASSIGNED in kyc.columns:
        names.update(kyc[K_ASSIGNED].dropna().astype(str).tolist())
    result["analysts"] = sorted(names)

    return result


def apply_filters(
    customers: pd.DataFrame,
    kyc: pd.DataFrame,
    analysts: pd.DataFrame,
    filtros: Filters,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Aplica filtros coherentes:
    - País/Sector/Riesgo → Customers y se propagan a KYC por Customer_ID.
    - Analista → KYC y Analysts.
    - Fechas → KYC (Creation_Date); opcionalmente Onboarding en Customers.
    """
    c = customers.copy()
    k = kyc.copy()
    a = analysts.copy()

    # Customers
    if not c.empty:
        mask = pd.Series(True, index=c.index)
        if filtros.country and C_COUNTRY in c.columns:
            mask &= c[C_COUNTRY].astype(str).isin(filtros.country)
        if filtros.sector and C_SECTOR in c.columns:
            mask &= c[C_SECTOR].astype(str).isin(filtros.sector)
        if filtros.risk and C_RISK_LEVEL in c.columns:
            mask &= c[C_RISK_LEVEL].astype(str).str.title().isin([r.title() for r in filtros.risk])
        if filtros.date_from is not None and C_ONBOARD in c.columns:
            mask &= (c[C_ONBOARD].notna()) & (c[C_ONBOARD] >= filtros.date_from)
        if filtros.date_to is not None and C_ONBOARD in c.columns:
            mask &= (c[C_ONBOARD].notna()) & (c[C_ONBOARD] <= filtros.date_to)
        c = c.loc[mask].copy()

    # Propagar selección de clientes a KYC
    if not k.empty and not c.empty and K_CUSTOMER_ID in k.columns and C_CUSTOMER_ID in c.columns:
        selected = set(c[C_CUSTOMER_ID].dropna().astype(str))
        k = k[k[K_CUSTOMER_ID].astype(str).isin(selected)].copy()

    # KYC
    if not k.empty:
        kmask = pd.Series(True, index=k.index)
        if filtros.analyst and K_ASSIGNED in k.columns:
            kmask &= k[K_ASSIGNED].astype(str).isin(filtros.analyst)
        if filtros.date_from is not None and K_CREATED in k.columns:
            kmask &= (k[K_CREATED].notna()) & (k[K_CREATED] >= filtros.date_from)
        if filtros.date_to is not None and K_CREATED in k.columns:
            kmask &= (k[K_CREATED].notna()) & (k[K_CREATED] <= filtros.date_to)
        k = k.loc[kmask].copy()

    # Analysts
    if not a.empty and filtros.analyst:
        if A_NAME in a.columns:
            a = a[a[A_NAME].astype(str).isin(filtros.analyst)].copy()
        elif A_ID in a.columns:
            a = a[a[A_ID].astype(str).isin(filtros.analyst)].copy()

    return c, k, a


# =============================================================================
# 3) Métricas/KPIs y agregaciones para los 8 puntos
# =============================================================================
# (Marcamos qué función alimenta cada punto del dashboard)

def compute_kpis(
    customers: pd.DataFrame,
    kyc: pd.DataFrame,
    analysts: pd.DataFrame,
    filtros: Optional[Filters] = None,
) -> Dict[str, float | int]:
    """
    KPIs superiores (Puntos 1–3 y parte de 6):
      - total_customers (1)
      - pct_high_risk (1)
      - kyc_completed (2)
      - kyc_pending (2)
      - avg_resolution_days (3)
      - avg_quality_score (6, media de QS en casos completos)
    """
    filtros = filtros or Filters()
    c, k, a = apply_filters(customers, kyc, analysts, filtros)

    out: Dict[str, float | int] = {}

    # (1) Total clientes y % Alto Riesgo
    total_customers = (
        c[C_CUSTOMER_ID].nunique()
        if (not c.empty and C_CUSTOMER_ID in c.columns)
        else (0 if c.empty else len(c))
    )
    out["total_customers"] = int(total_customers)
    if not c.empty and C_RISK_LEVEL in c.columns and total_customers:
        high = (c[C_RISK_LEVEL].astype(str).str.strip().str.lower() == "high").sum()
        out["pct_high_risk"] = round(_safe_div(high, total_customers) * 100.0, 2)
    else:
        out["pct_high_risk"] = np.nan

    # (2) Completados y Pendientes
    completed = pending = 0
    if not k.empty and K_STATUS in k.columns:
        status = k[K_STATUS].astype(str)
        completed = (status == "Complete").sum()
        pending   = status.isin(list(PENDING_SET)).sum()

    out["kyc_completed"] = int(completed)
    out["kyc_pending"]   = int(pending)

    # (3) Tiempo medio de resolución (días) — solo casos completos
    avg_pt = np.nan
    if not k.empty and K_PT_DAYS in k.columns and K_STATUS in k.columns:
        done_mask = k[K_STATUS].astype(str) == "Complete"
        vals = pd.to_numeric(k.loc[done_mask, K_PT_DAYS], errors="coerce").dropna()
        avg_pt = round(vals.mean(), 2) if not vals.empty else np.nan
    out["avg_resolution_days"] = avg_pt

    # (6) Calidad media de casos (Quality Score sobre completos)
    avg_q = np.nan
    if not k.empty and K_QUALITY in k.columns and K_STATUS in k.columns:
        qs = pd.to_numeric(k.loc[k[K_STATUS].astype(str) == "Complete", K_QUALITY], errors="coerce").dropna()
        avg_q = round(qs.mean(), 2) if not qs.empty else np.nan
    out["avg_quality_score"] = avg_q

    return out


def documentation_completion_rate(customers: pd.DataFrame, filtros: Optional[Filters] = None) -> float:
    """
    KPI adicional (no numerado en el Word pero útil en KPI-bar):
      % de clientes con documentación completa (Customers.Documentation_Complete).
    """
    filtros = filtros or Filters()
    c, _, _ = apply_filters(customers, pd.DataFrame(), pd.DataFrame(), filtros)
    if c.empty or "Documentation_Complete" not in c.columns:
        return np.nan
    s = c["Documentation_Complete"].astype(str).str.strip().str.lower()
    ok = s.isin({"true", "yes", "1", "y", "si", "sí"})
    total = len(s)
    return round(float(ok.sum()) / total * 100.0, 2) if total else np.nan


def risk_distribution(customers: pd.DataFrame, filtros: Optional[Filters] = None) -> pd.DataFrame:
    """
    (1) Distribución de clientes por nivel de riesgo (High/Medium/Low):
      -> DataFrame: Risk_Level, Count
    """
    filtros = filtros or Filters()
    c, _, _ = apply_filters(customers, pd.DataFrame(), pd.DataFrame(), filtros)
    if c.empty or C_RISK_LEVEL not in c.columns:
        return pd.DataFrame(columns=[C_RISK_LEVEL, "Count"]).astype({"Count": "int64"})
    ser = c[C_RISK_LEVEL].astype(str).str.title()
    return ser.value_counts(dropna=True).rename_axis(C_RISK_LEVEL).reset_index(name="Count")


def risk_by_sector(customers: pd.DataFrame, filtros: Optional[Filters] = None) -> pd.DataFrame:
    """
    (1) Riesgo por Sector:
      -> DataFrame: Sector | High | Medium | Low | Total | % High Risk
    """
    filtros = filtros or Filters()
    c, _, _ = apply_filters(customers, pd.DataFrame(), pd.DataFrame(), filtros)
    if c.empty or C_SECTOR not in c.columns or C_RISK_LEVEL not in c.columns:
        return pd.DataFrame(columns=[C_SECTOR, "High", "Medium", "Low", "Total", "% High Risk"])

    tmp = c.copy()
    tmp[C_RISK_LEVEL] = tmp[C_RISK_LEVEL].astype(str).str.title()
    pivot = (
        tmp.pivot_table(
            index=C_SECTOR,
            columns=C_RISK_LEVEL,
            values=(C_CUSTOMER_ID if C_CUSTOMER_ID in tmp.columns else tmp.columns[0]),
            aggfunc="nunique",
            fill_value=0,
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )
    for col in ["High", "Medium", "Low"]:
        if col not in pivot.columns:
            pivot[col] = 0
    pivot["Total"] = pivot[["High", "Medium", "Low"]].sum(axis=1)
    pivot["% High Risk"] = np.where(pivot["Total"] > 0, (pivot["High"] / pivot["Total"]) * 100.0, np.nan).round(2)
    return pivot[[C_SECTOR, "High", "Medium", "Low", "Total", "% High Risk"]].sort_values(
        by=["% High Risk", "Total"], ascending=[False, False]
    )


def kyc_status_breakdown(
    kyc: pd.DataFrame, filtros: Optional[Filters] = None, customers_for_cross: Optional[pd.DataFrame] = None
) -> pd.DataFrame:
    """
    (2) Estados de casos KYC:
      -> DataFrame: Status, Count  (orden: Complete, Pending, Under_Review, Escalated)
    """
    filtros = filtros or Filters()
    if customers_for_cross is None:
        _, k, _ = apply_filters(pd.DataFrame(), kyc, pd.DataFrame(), filtros)
    else:
        _, k, _ = apply_filters(customers_for_cross, kyc, pd.DataFrame(), filtros)

    if k.empty or K_STATUS not in k.columns:
        return pd.DataFrame(columns=[K_STATUS, "Count"])

    df = k[K_STATUS].value_counts().rename_axis(K_STATUS).reset_index(name="Count")
    cat_order = ["Complete", "Pending", "Under_Review", "Escalated"]
    df[K_STATUS] = pd.Categorical(df[K_STATUS], categories=cat_order, ordered=True)
    return df.sort_values(K_STATUS).reset_index(drop=True)


def dependencies_analysis(
    kyc: pd.DataFrame, filtros: Optional[Filters] = None, customers_for_cross: Optional[pd.DataFrame] = None
) -> pd.DataFrame:
    """
    (4) Pendientes por dependencia (solo Status != Complete):
      -> DataFrame: Dependency | Cases | % | Avg_Days
    """
    filtros = filtros or Filters()
    if customers_for_cross is None:
        _, k, _ = apply_filters(pd.DataFrame(), kyc, pd.DataFrame(), filtros)
    else:
        _, k, _ = apply_filters(customers_for_cross, kyc, pd.DataFrame(), filtros)

    if k.empty or K_STATUS not in k.columns or K_DEP not in k.columns:
        return pd.DataFrame(columns=[K_DEP, "Cases", "%", "Avg_Days"])

    pending = k[k[K_STATUS].isin(PENDING_SET)].copy()
    if pending.empty:
        return pd.DataFrame(columns=[K_DEP, "Cases", "%", "Avg_Days"])

    dep_counts = pending.groupby(K_DEP, dropna=False).size().rename("Cases").reset_index()
    total = dep_counts["Cases"].sum()
    dep_counts["%"] = (dep_counts["Cases"] / total * 100.0).round(2)

    if K_PT_DAYS in pending.columns:
        avg_days = (
            pending.groupby(K_DEP, dropna=False)[K_PT_DAYS]
            .mean()
            .round(2)
            .reset_index()
            .rename(columns={K_PT_DAYS: "Avg_Days"})
        )
        dep_counts = dep_counts.merge(avg_days, on=K_DEP, how="left")
    else:
        dep_counts["Avg_Days"] = np.nan

    return dep_counts.sort_values("Cases", ascending=False).reset_index(drop=True)


def trends_monthly(
    kyc: pd.DataFrame, filtros: Optional[Filters] = None, customers_for_cross: Optional[pd.DataFrame] = None
) -> pd.DataFrame:
    """
    (5) Tendencias mensuales:
      -> DataFrame: Month | Nuevos | Completados | Pendientes
    """
    filtros = filtros or Filters()
    if customers_for_cross is None:
        _, k, _ = apply_filters(pd.DataFrame(), kyc, pd.DataFrame(), filtros)
    else:
        _, k, _ = apply_filters(customers_for_cross, kyc, pd.DataFrame(), filtros)

    if k.empty:
        return pd.DataFrame(columns=["Month", "Nuevos", "Completados", "Pendientes"])

    # Nuevos por mes (Creation_Date)
    new_by_month = pd.Series(dtype="int64")
    if K_CREATED in k.columns:
        new = k.dropna(subset=[K_CREATED]).copy()
        if not new.empty:
            new["Month"] = new[K_CREATED].values.astype("datetime64[M]")
            new_by_month = new.groupby("Month").size().rename("Nuevos")

    # Completados por mes (Completion_Date)
    completed_by_month = pd.Series(dtype="int64")
    if K_COMPLETED in k.columns and K_STATUS in k.columns:
        comp = k[(k[K_STATUS] == "Complete") & k[K_COMPLETED].notna()].copy()
        if not comp.empty:
            comp["Month"] = comp[K_COMPLETED].values.astype("datetime64[M]")
            completed_by_month = comp.groupby("Month").size().rename("Completados")

    # Pendientes (snapshot fin de mes)
    months = pd.Index([])
    if not new_by_month.empty:        months = months.union(new_by_month.index)
    if not completed_by_month.empty:  months = months.union(completed_by_month.index)
    months = months.sort_values()

    pending_series = pd.Series(dtype="int64")
    if len(months) > 0:
        counts = []
        for m in months:
            month_end = (pd.Timestamp(m) + pd.offsets.MonthEnd(0))
            created_up_to = k.loc[k[K_CREATED].notna() & (k[K_CREATED] <= month_end)].shape[0] if K_CREATED in k.columns else 0
            completed_up_to = (
                k.loc[(k[K_STATUS] == "Complete") & k[K_COMPLETED].notna() & (k[K_COMPLETED] <= month_end)].shape[0]
                if (K_COMPLETED in k.columns and K_STATUS in k.columns) else 0
            )
            counts.append(created_up_to - completed_up_to)
        pending_series = pd.Series(counts, index=months, name="Pendientes")

    trend = pd.concat([new_by_month, completed_by_month, pending_series], axis=1).fillna(0).astype(int)
    return trend.reset_index().rename(columns={"index": "Month"})


def analysts_summary(
    analysts: pd.DataFrame, kyc: pd.DataFrame, filtros: Optional[Filters] = None
) -> pd.DataFrame:
    """
    (6) Productividad/Calidad de Analistas (para ranking/gráficos):
      -> DataFrame: Analyst_Name | Cases_Per_Month | Quality_Score | Performance_Rating | Avg_Resolution_Days
    """
    filtros = filtros or Filters()
    _, k, a = apply_filters(pd.DataFrame(), kyc, analysts, filtros)
    if a.empty:
        return pd.DataFrame(columns=[A_NAME, A_CASES_PM, A_QUALITY, A_PERF, "Avg_Resolution_Days"])

    out = a.copy()
    for col in [A_NAME, A_CASES_PM, A_QUALITY, A_PERF]:
        if col not in out.columns:
            out[col] = np.nan

    if "Avg_Resolution_Days" not in out.columns:
        if A_AVG_HOURS in out.columns:
            with np.errstate(invalid="ignore", divide="ignore"):
                out["Avg_Resolution_Days"] = pd.to_numeric(out[A_AVG_HOURS], errors="coerce") / 24.0
        else:
            out["Avg_Resolution_Days"] = np.nan

    perf_norm = _norm_str_series(out[A_PERF]) if A_PERF in out.columns else pd.Series("", index=out.index)
    perf_rank = perf_norm.isin({"top performer", "excellent"}).astype(int)
    out = out.assign(_perf_rank=perf_rank).sort_values(
        by=["_perf_rank", A_CASES_PM, A_QUALITY],
        ascending=[False, False, False],
        na_position="last",
    ).drop(columns=["_perf_rank"])

    return out[[A_NAME, A_CASES_PM, A_QUALITY, A_PERF, "Avg_Resolution_Days"]].reset_index(drop=True)


def risk_rating_distribution(
    kyc: pd.DataFrame, filtros: Optional[Filters] = None, customers_for_cross: Optional[pd.DataFrame] = None
) -> pd.DataFrame:
    """
    (8) Distribución de Risk Rating (KYC_Cases):
      -> DataFrame: RatingLabel, Count   (acepta 1/2/3 o High/Medium/Low)
    """
    filtros = filtros or Filters()
    if customers_for_cross is None:
        _, k, _ = apply_filters(pd.DataFrame(), kyc, pd.DataFrame(), filtros)
    else:
        _, k, _ = apply_filters(customers_for_cross, kyc, pd.DataFrame(), filtros)

    if k.empty or K_RISK_RATING not in k.columns:
        return pd.DataFrame(columns=["RatingLabel", "Count"])

    rr = k[K_RISK_RATING].astype(str).str.strip().str.lower()
    mapping_num = {"1": "High", "2": "Medium", "3": "Low"}
    mapping_txt = {"high": "High", "medium": "Medium", "low": "Low"}
    label = rr.map(lambda x: mapping_num.get(x, mapping_txt.get(x, "Unknown")))
    df = label.value_counts().rename_axis("RatingLabel").reset_index(name="Count")
    order = pd.Categorical(df["RatingLabel"], categories=["High", "Medium", "Low", "Unknown"], ordered=True)
    df["RatingLabel"] = order
    return df.sort_values("RatingLabel").reset_index(drop=True)


def quality_score_bands_cases(
    kyc: pd.DataFrame, filtros: Optional[Filters] = None, customers_for_cross: Optional[pd.DataFrame] = None
) -> pd.DataFrame:
    """
    (7) Bandas de Quality Score para CASOS completados:
      -> DataFrame: Band, Count   (70–79 / 80–89 / 90–99 / Otros)
    """
    filtros = filtros or Filters()
    if customers_for_cross is None:
        _, k, _ = apply_filters(pd.DataFrame(), kyc, pd.DataFrame(), filtros)
    else:
        _, k, _ = apply_filters(customers_for_cross, kyc, pd.DataFrame(), filtros)

    if k.empty or K_QUALITY not in k.columns or K_STATUS not in k.columns:
        return pd.DataFrame(columns=["Band", "Count"])

    comp = k[k[K_STATUS] == "Complete"].copy()
    q = pd.to_numeric(comp[K_QUALITY], errors="coerce").dropna()

    def band(v: float) -> str:
        if 70 <= v < 80:  return "70–79"
        if 80 <= v < 90:  return "80–89"
        if 90 <= v <= 99: return "90–99"
        return "Otros"

    labels = q.map(band)   # <-- FIX: evitar sombredo de nombre
    out = labels.value_counts().rename_axis("Band").reset_index(name="Count")
    order = pd.Categorical(out["Band"], categories=["70–79", "80–89", "90–99", "Otros"], ordered=True)
    out["Band"] = order
    return out.sort_values("Band").reset_index(drop=True)


def quality_score_bands_analysts(
    analysts: pd.DataFrame, filtros: Optional[Filters] = None
) -> pd.DataFrame:
    """
    (7) Bandas de Quality Score para ANALISTAS:
      -> DataFrame: Band, Count   (70–79 / 80–89 / 90–99 / Otros)
    """
    filtros = filtros or Filters()
    _, _, a = apply_filters(pd.DataFrame(), pd.DataFrame(), analysts, filtros)
    if a.empty or A_QUALITY not in a.columns:
        return pd.DataFrame(columns=["Band", "Count"])

    q = pd.to_numeric(a[A_QUALITY], errors="coerce").dropna()

    def band(v: float) -> str:
        if 70 <= v < 80:  return "70–79"
        if 80 <= v < 90:  return "80–89"
        if 90 <= v <= 99: return "90–99"
        return "Otros"

    labels = q.map(band)   # <-- FIX: evitar sombredo de nombre
    out = labels.value_counts().rename_axis("Band").reset_index(name="Count")
    order = pd.Categorical(out["Band"], categories=["70–79", "80–89", "90–99", "Otros"], ordered=True)
    out["Band"] = order
    return out.sort_values("Band").reset_index(drop=True)
