# app.py
# UI Streamlit — toda la lógica de datos/calculada está en main.py

import streamlit as st
import pandas as pd
import altair as alt

from main import (
    load_all,
    build_filters,
    compute_kpis,
    Filters,
    # Riesgo (clientes) + Risk Rating de casos
    risk_distribution,
    risk_by_sector,
    risk_rating_distribution,
    # Operativa KYC
    kyc_status_breakdown,
    dependencies_analysis,
    trends_monthly,
    # Analistas y Calidad
    analysts_summary,
    quality_score_bands_cases,
    quality_score_bands_analysts,
)

# =========================
# PALETA (morados + azules)
# =========================
ACCENT = "#7C3AED"   # morado principal
MORADOS = ["#6D28D9", "#7C3AED", "#A78BFA", "#C4B5FD"]   # oscuro → claro
AZULES  = ["#1E3A8A", "#2563EB", "#60A5FA", "#93C5FD"]   # oscuro → claro
GRIS    = "#9CA3AF"

def scale_risk_levels():
    # Orden consistente High > Medium > Low > Unknown
    return alt.Scale(
        domain=["High", "Medium", "Low", "Unknown"],
        range=[MORADOS[0], MORADOS[2], AZULES[2], GRIS]
    )

def scale_gradient():
    # Gradiente para intensidades (% u otros)
    return alt.Scale(range=[AZULES[3], AZULES[2], MORADOS[2], MORADOS[0]])

def scale_trends():
    return alt.Scale(
        domain=["Nuevos", "Completados", "Pendientes"],
        range=[AZULES[2], MORADOS[2], MORADOS[0]],
    )

def scale_status():
    return alt.Scale(
        domain=["Complete", "Pending", "Under_Review", "Escalated"],
        range=[AZULES[2], MORADOS[2], MORADOS[0], AZULES[0]],
    )


# ----------------------------
# CONFIGURACIÓN DE LA PÁGINA
# ----------------------------
st.set_page_config(
    page_title="Compliance 360 – KYC (Vista única)",
    page_icon="data/accenturelogo.png",   # logo como icono
    layout="wide",
    initial_sidebar_state="expanded",
)

# Logo en sidebar (si está disponible)
try:
    st.sidebar.image("data/accenturelogo1.png", use_container_width=True)
except Exception:
    pass

st.title("AML Dashboard – KYC")

# ----------------------------
# CARGA DE DATOS (CACHEADA)
# ----------------------------
@st.cache_data(show_spinner=True)
def _load():
    return load_all()  # usa data/AML_KPIs_PowerBI_Complete.xlsx por defecto

try:
    dfs = _load()
    customers = dfs.get("customers", pd.DataFrame())
    kyc = dfs.get("kyc", pd.DataFrame())
    analysts = dfs.get("analysts", pd.DataFrame())
except FileNotFoundError as e:
    st.error(str(e))
    st.stop()

if customers.empty and kyc.empty and analysts.empty:
    st.warning("No se pudieron cargar hojas válidas del Excel.")
    st.stop()

# ----------------------------
# SIDEBAR – FILTROS GLOBALES
# ----------------------------
st.sidebar.header("Filtros")

avail = build_filters(customers, kyc, analysts)

date_from = st.sidebar.date_input("🗓️ Desde", value=None)
date_to = st.sidebar.date_input("🗓️ Hasta", value=None)

def _to_ts(d):
    if d is None or (isinstance(d, list) and not d):
        return None
    return pd.Timestamp(d)

flt = Filters(
    date_from=_to_ts(date_from),
    date_to=_to_ts(date_to),
    country=st.sidebar.multiselect("🌍 País", options=avail.get("countries", [])),
    sector=st.sidebar.multiselect("🏭 Sector", options=avail.get("sectors", [])),
    risk=st.sidebar.multiselect("⚠️ Riesgo (cliente)", options=avail.get("risk_levels", [])),
    analyst=st.sidebar.multiselect("👩‍💼 Analista", options=avail.get("analysts", [])),
)

# Privacidad/neutralidad en “Equipo y calidad”
anonymize_analysts = st.sidebar.toggle(
    "Anonimizar analistas",
    value=True,
    help="Oculta nombres propios en los gráficos de equipo."
)

# ----------------------------
# KPIs PRINCIPALES (arriba)
# ----------------------------
def _fmt(value, pct=False, count=False):
    if pd.isna(value) or value is None:
        return "N/A"
    if count:
        return f"{int(value):,}"
    return f"{value:.2f}%" if pct else f"{value:.2f}"

kpis = compute_kpis(customers, kyc, analysts, flt)

k = st.columns(6)
k[0].metric("Total clientes", _fmt(kpis.get("total_customers"), count=True))
k[1].metric("% alto riesgo", _fmt(kpis.get("pct_high_risk"), pct=True))
k[2].metric("KYC completados", _fmt(kpis.get("kyc_completed"), count=True))
k[3].metric("KYC pendientes", _fmt(kpis.get("kyc_pending"), count=True))
k[4].metric("Tiempo medio (días)", _fmt(kpis.get("avg_resolution_days")))
k[5].metric("Calidad media (casos)", _fmt(kpis.get("avg_quality_score")))

st.markdown("---")

# =========================
# BLOQUE: RIESGO DE CLIENTES
# =========================
st.subheader("Riesgo de clientes")

# ── Fila 1: Distribución + Risk Rating
col_r1, col_r3 = st.columns([1, 1])

with col_r1:
    df_risk = risk_distribution(customers, flt)
    if not df_risk.empty:
        chart_risk = (
            alt.Chart(df_risk)
            .mark_bar()
            .encode(
                x=alt.X("Risk_Level:N", title="Nivel de riesgo",
                        sort=["High", "Medium", "Low", "Unknown"]),
                y=alt.Y("Count:Q", title="Clientes"),
                color=alt.Color("Risk_Level:N", legend=None, scale=scale_risk_levels()),
                tooltip=["Risk_Level", alt.Tooltip("Count:Q", format=",")],
            )
            .properties(height=280, title="Distribución por nivel de riesgo")
        )
        st.altair_chart(chart_risk, use_container_width=True)
    else:
        st.info("Sin datos suficientes para la distribución de riesgo.")

with col_r3:
    df_rr = risk_rating_distribution(kyc, flt, customers_for_cross=customers)
    if not df_rr.empty:
        chart_rr = (
            alt.Chart(df_rr)
            .mark_bar()
            .encode(
                x=alt.X("RatingLabel:N", title="Risk Rating",
                        sort=["High", "Medium", "Low", "Unknown"]),
                y=alt.Y("Count:Q", title="Casos"),
                color=alt.Color("RatingLabel:N", legend=None, scale=scale_risk_levels()),
                tooltip=["RatingLabel", alt.Tooltip("Count:Q", format=",")],
            )
            .properties(height=280, title="Risk Rating")
        )
        st.altair_chart(chart_rr, use_container_width=True)
    else:
        st.info("Sin datos de Risk Rating en casos.")

# ── Fila 2: Riesgo por sector en ancho completo (estética "pendientes por dependencia")
df_sector = risk_by_sector(customers, flt)
if not df_sector.empty:
    # Excluir sectores nulos y ordenar por % High Risk desc
    df_sector = df_sector[df_sector["Sector"].notna()].copy()
    df_sector = df_sector.sort_values(by="% High Risk", ascending=False)

    chart_sector_like_dep = (
        alt.Chart(df_sector.head(12))  # Top 12 sectores
        .mark_bar()
        .encode(
            x=alt.X("Total:Q", title="Clientes (Top 12)"),
            y=alt.Y("Sector:N", sort="-x", title="Sector"),
            color=alt.Color("% High Risk:Q", title="% High Risk", scale=scale_gradient()),
            tooltip=[
                "Sector:N",
                alt.Tooltip("High:Q", format=",", title="High"),
                alt.Tooltip("Medium:Q", format=",", title="Medium"),
                alt.Tooltip("Low:Q", format=",", title="Low"),
                alt.Tooltip("Total:Q", format=",", title="Total"),
                alt.Tooltip("% High Risk:Q", format=".2f", title="% High Risk"),
            ],
        )
        .properties(height=360, title="Riesgo por sector")
    )
    st.altair_chart(chart_sector_like_dep, use_container_width=True)
else:
    st.info("Sin datos de sectores.")

st.markdown("---")

# ==================
# BLOQUE: OPERATIVA
# ==================
st.subheader("Operativa KYC")
col_o1, col_o2 = st.columns([1, 1])

with col_o1:
    status_df = kyc_status_breakdown(kyc, flt, customers_for_cross=customers)
    if not status_df.empty:
        chart_status = (
            alt.Chart(status_df)
            .mark_bar()
            .encode(
                x=alt.X("Status:N",
                        sort=["Complete", "Pending", "Under_Review", "Escalated"],
                        title="Estado"),
                y=alt.Y("Count:Q", title="Casos"),
                color=alt.Color("Status:N", legend=None, scale=scale_status()),
                tooltip=["Status", alt.Tooltip("Count:Q", format=",")],
            )
            .properties(height=320, title="Estados de casos")
        )
        st.altair_chart(chart_status, use_container_width=True)
    else:
        st.info("No hay casos con los filtros actuales.")

with col_o2:
    dep_df = dependencies_analysis(kyc, flt, customers_for_cross=customers)
    if not dep_df.empty:
        # ⛔ Excluir nulos y vacíos en Dependency
        dep_df = dep_df[dep_df["Dependency"].notna()].copy()
        dep_df = dep_df[dep_df["Dependency"].astype(str).str.strip() != ""]
        chart_dep = (
            alt.Chart(dep_df.head(10))
            .mark_bar()
            .encode(
                x=alt.X("Cases:Q", title="Casos (Top 10)"),
                y=alt.Y("Dependency:N", sort="-x", title="Dependencia"),
                color=alt.Color("%:Q", title="% del total", scale=scale_gradient()),
                tooltip=[
                    "Dependency:N",
                    alt.Tooltip("Cases:Q", format=","),
                    alt.Tooltip("%:Q", format=".2f"),
                    alt.Tooltip("Avg_Days:Q", format=".2f", title="Días medios"),
                ],
            )
            .properties(height=320, title="Pendientes por dependencia")
        )
        st.altair_chart(chart_dep, use_container_width=True)
    else:
        st.info("No hay pendientes por dependencia.")

trend_df = trends_monthly(kyc, flt, customers_for_cross=customers)
if not trend_df.empty:
    base = alt.Chart(trend_df).transform_fold(
        ["Nuevos", "Completados", "Pendientes"], as_=["Tipo", "Casos"]
    )
    line = (
        base.mark_line(point=True)
        .encode(
            x=alt.X("Month:T", title="Mes"),
            y=alt.Y("Casos:Q", title="Número de casos"),
            color=alt.Color("Tipo:N", scale=scale_trends()),
            tooltip=[alt.Tooltip("Month:T", title="Mes"), "Tipo:N", alt.Tooltip("Casos:Q", format=",")],
        )
        .properties(height=360, title="Tendencias mensuales")
    )
    st.altair_chart(line, use_container_width=True)
else:
    st.info("No hay datos de tendencias.")

st.markdown("---")

# =======================
# BLOQUE: EQUIPO Y CALIDAD
# =======================
st.subheader("Equipo y calidad")
col_a1, col_a2 = st.columns([1, 1])

# Para anonimizar nombres en el scatter y en bandas:
def _maybe_anon(df: pd.DataFrame, col_name: str = "Analyst_Name") -> pd.DataFrame:
    if df.empty or col_name not in df.columns:
        return df
    if anonymize_analysts:
        # Mapeo determinista por orden de aparición
        uniq = pd.Series(df[col_name].astype(str).unique())
        mapping = {name: f"Analyst #{i+1}" for i, name in enumerate(uniq)}
        out = df.copy()
        out[col_name] = out[col_name].astype(str).map(mapping)
        return out
    return df

with col_a1:
    ana_df = analysts_summary(analysts, kyc, flt)
    if not ana_df.empty:
        ana_df = _maybe_anon(ana_df, "Analyst_Name")

        # --- Reglas de referencia: medias X/Y y umbral Y=90 ---
        mean_x = alt.Chart(ana_df).mark_rule(strokeDash=[4, 4], color=GRIS).encode(
            x='mean(Cases_Per_Month):Q'
        )
        mean_y = alt.Chart(ana_df).mark_rule(strokeDash=[4, 4], color=GRIS).encode(
            y='mean(Quality_Score):Q'
        )
        threshold_y = alt.Chart(pd.DataFrame({'y': [90]})).mark_rule(color=AZULES[1]).encode(
            y='y:Q'
        )

        # Scatter neutral: sin Performance_Rating; más legible con opacidad+contorno
        scatter_perf = (
            alt.Chart(ana_df)
            .mark_circle(size=140, opacity=0.6, fill=MORADOS[2], stroke=MORADOS[0], strokeWidth=1)
            .encode(
                x=alt.X("Cases_Per_Month:Q", title="Casos/mes"),
                y=alt.Y("Quality_Score:Q", title="Quality Score", scale=alt.Scale(domain=[70, 100])),
                tooltip=[
                    "Analyst_Name:N",
                    alt.Tooltip("Cases_Per_Month:Q", format=".1f", title="Casos/mes"),
                    alt.Tooltip("Quality_Score:Q", format=".2f", title="Calidad"),
                    alt.Tooltip("Avg_Resolution_Days:Q", format=".1f", title="Días medios"),
                ],
            )
            .properties(height=320, title="Actividad vs calidad")
        )

        composed = (scatter_perf + mean_x + mean_y + threshold_y).resolve_scale(color='independent')
        st.altair_chart(composed, use_container_width=True)
    else:
        st.info("Sin datos de analistas con los filtros actuales.")

with col_a2:
    df_qa = quality_score_bands_analysts(analysts, flt)
    if not df_qa.empty:
        # Bandas por analistas (no muestra nombres)
        chart_qa = (
            alt.Chart(df_qa)
            .mark_bar()
            .encode(
                x=alt.X("Band:N", title="Banda", sort=["70–79", "80–89", "90–99", "Otros"]),
                y=alt.Y("Count:Q", title="Analistas"),
                color=alt.Color(
                    "Band:N",
                    scale=alt.Scale(
                        domain=["70–79", "80–89", "90–99", "Otros"],
                        range=[AZULES[2], MORADOS[2], MORADOS[0], GRIS]
                    ),
                    legend=None
                ),
                tooltip=["Band", alt.Tooltip("Count:Q", format=",")],
            )
            .properties(height=360, title="Bandas de calidad")
        )
        st.altair_chart(chart_qa, use_container_width=True)
    else:
        st.info("No hay calidad de analistas para agrupar en bandas.")

df_qc = quality_score_bands_cases(kyc, flt, customers_for_cross=customers)
if not df_qc.empty:
    chart_qc = (
        alt.Chart(df_qc)
        .mark_bar()
        .encode(
            x=alt.X("Band:N", title="Banda", sort=["70–79", "80–89", "90–99", "Otros"]),
            y=alt.Y("Count:Q", title="Casos"),
            color=alt.Color(
                "Band:N",
                scale=alt.Scale(
                    domain=["70–79", "80–89", "90–99", "Otros"],
                    range=[AZULES[2], MORADOS[2], MORADOS[0], GRIS]
                ),
                legend=None
            ),
            tooltip=["Band", alt.Tooltip("Count:Q", format=",")],
        )
        .properties(height=300, title="Bandas de calidad")
    )
    st.altair_chart(chart_qc, use_container_width=True)
else:
    st.info("No hay calidad (casos) para agrupar en bandas.")
