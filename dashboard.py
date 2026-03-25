"""
Data Migration Quality Dashboard
Streamlit + DuckDB visualization layer for the data_eng_exercise pipeline.
Queries the pipeline database views directly — no pipeline re-run needed.

Run: streamlit run dashboard.py
"""

import os
import numpy as np
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import duckdb
from scipy import stats
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("DUCKDB_PATH", "/mnt/e/Data Eng Exercise/data_eng.duckdb")

st.set_page_config(
    page_title="Data Migration Quality Dashboard",
    page_icon="📊",
    layout="wide",
)


# ── DuckDB connection (read-only) ─────────────────────────────────────────────

@st.cache_resource
def get_db():
    """Read-only connection to the pipeline DuckDB database."""
    return duckdb.connect(DB_PATH, read_only=True)


@st.cache_data
def query(sql: str):
    """Execute a SQL query against the pipeline database and return a DataFrame."""
    return get_db().execute(sql).fetchdf()


# ── Six Sigma helpers ─────────────────────────────────────────────────────────

def accurate_sigma(yield_value: float) -> float:
    """Recalculate sigma level using scipy inverse normal (matches pipeline logic)."""
    if yield_value <= 0 or yield_value >= 1:
        return float("nan")
    return stats.norm.ppf(yield_value) + 1.5


def sigma_badge(sigma: float) -> str:
    if sigma >= 5.0:  return "🏆"
    elif sigma >= 4.5: return "🟢"
    elif sigma >= 4.0: return "🟡"
    else:              return "🔴"


# ── Page header ───────────────────────────────────────────────────────────────

st.title("📊 Data Migration Quality Dashboard")
st.caption("CMS DE-SynPUF · Legacy → New System · Six Sigma Analysis · DuckDB + Streamlit")

# Load summary and recalculate sigma with scipy (same as pipeline)
summary = query("SELECT * FROM vw_sigma_analysis")
summary["SIGMA_LEVEL"] = summary["YIELD"].apply(accurate_sigma)

carrier_row = summary[summary["SUBJECT"] == "Carrier Claims"].iloc[0]
bene_row    = summary[summary["SUBJECT"] == "Beneficiary Summary"].iloc[0]

tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9 = st.tabs([
    "🎯 Migration Scorecard",
    "📉 Six Sigma by Field",
    "💵 Financial Impact",
    "🔍 Discrepancy Drill-Down",
    "📦 Row Count Validation",
    "🔥 Defect Heatmap",
    "🏥 Chronic Condition Errors",
    "📅 Year-over-Year Trend",
    "🚦 Go / No-Go Summary",
])


# ── Tab 1: Migration Scorecard ────────────────────────────────────────────────

with tab1:
    st.subheader("Overall Migration Quality")

    col1, col2 = st.columns(2)
    for col, row, label in [
        (col1, carrier_row, "Carrier Claims"),
        (col2, bene_row,    "Beneficiary Summary"),
    ]:
        with col:
            st.markdown(f"### {sigma_badge(row['SIGMA_LEVEL'])} {label}")
            m1, m2, m3 = st.columns(3)
            m1.metric("Sigma Level",   f"{row['SIGMA_LEVEL']:.2f}")
            m2.metric("Process Yield", f"{row['YIELD'] * 100:.4f}%")
            m3.metric("DPMO",          f"{row['DPMO']:,.0f}")
            st.caption(
                f"Total units: {row['TOTAL_UNITS']:,} · "
                f"Defects: {row['TOTAL_DEFECTS']:,}"
            )

    st.divider()
    st.subheader("Sigma Level Reference")
    ref = query("""
        SELECT * FROM (VALUES
            ('6σ', '3.4',    '99.9997%', '🏆 World-class'),
            ('5σ', '233',    '99.977%',  '🟢 Excellent'),
            ('4.5σ','1,350', '99.865%',  '🟢 Strong'),
            ('4σ', '6,210',  '99.379%',  '🟡 Acceptable'),
            ('3σ', '66,807', '93.319%',  '🔴 Needs work')
        ) AS t(sigma_level, dpmo, yield_pct, status)
    """)
    st.dataframe(ref, hide_index=True, use_container_width=True)


# ── Tab 2: Six Sigma by Field ─────────────────────────────────────────────────

with tab2:
    st.subheader("Defect Rate by Field Family")

    subject = st.radio("Subject area",
                       ["Carrier Claims", "Beneficiary Summary"],
                       horizontal=True)

    view = ("vw_sigma_analysis_carrier_columns" if subject == "Carrier Claims"
            else "vw_sigma_analysis_beneficiary_columns")

    top_n = st.slider("Show top N fields by defect count", 5, 30, 15)

    df_field = query(f"""
        SELECT FIELD_FAMILY, TOTAL_DEFECTS, DPMO, SIGMA_LEVEL, RUNNING_PCT_OF_TOTAL
        FROM {view}
        ORDER BY TOTAL_DEFECTS DESC
        LIMIT {top_n}
    """)
    df_field = df_field.sort_values("SIGMA_LEVEL")

    fig = px.bar(
        df_field,
        x="SIGMA_LEVEL",
        y="FIELD_FAMILY",
        orientation="h",
        color="SIGMA_LEVEL",
        color_continuous_scale=["red", "orange", "green"],
        range_color=[3.0, 5.5],
        labels={"SIGMA_LEVEL": "Sigma Level", "FIELD_FAMILY": "Field"},
        title=f"Sigma Level by Field — {subject} (top {top_n} by defect count)",
        text=df_field["SIGMA_LEVEL"].round(2),
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(coloraxis_showscale=False, height=max(400, top_n * 28))
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Full data table"):
        df_full = query(f"""
            SELECT FIELD_FAMILY, TOTAL_DEFECTS, DPMO, SIGMA_LEVEL, RUNNING_PCT_OF_TOTAL
            FROM {view}
            ORDER BY TOTAL_DEFECTS DESC
        """)
        st.dataframe(
            df_full,
            hide_index=True,
            use_container_width=True,
            column_config={
                "DPMO":                 st.column_config.NumberColumn(format="%d"),
                "TOTAL_DEFECTS":        st.column_config.NumberColumn(format="%d"),
                "SIGMA_LEVEL":          st.column_config.NumberColumn(format="%.3f"),
                "RUNNING_PCT_OF_TOTAL": st.column_config.ProgressColumn(
                    min_value=0, max_value=1, format="%.1%%"
                ),
            },
        )


# ── Tab 3: Financial Impact ───────────────────────────────────────────────────

with tab3:
    st.subheader("Financial Variance — Pareto Analysis")

    dataset = st.radio("Dataset",
                       ["Carrier Claims", "Beneficiary Summary"],
                       horizontal=True)

    fin_view = ("vw_claim_financial_error_impact" if dataset == "Carrier Claims"
                else "vw_beneficiary_financial_error_impact")

    df_fin = query(f"""
        SELECT FIELD_FAMILY, FINANCIAL_VARIANCE, RUNNING_PCT_OF_TOTAL
        FROM {fin_view}
        ORDER BY FINANCIAL_VARIANCE DESC
    """)

    total_variance = df_fin["FINANCIAL_VARIANCE"].sum()

    fig2 = go.Figure()
    fig2.add_trace(go.Bar(
        x=df_fin["FIELD_FAMILY"],
        y=df_fin["FINANCIAL_VARIANCE"],
        name="Variance ($)",
        marker_color="#1f77b4",
        text=df_fin["FINANCIAL_VARIANCE"].apply(lambda v: f"${v:,.0f}"),
        textposition="outside",
    ))
    fig2.add_trace(go.Scatter(
        x=df_fin["FIELD_FAMILY"],
        y=df_fin["RUNNING_PCT_OF_TOTAL"] * 100,
        name="Cumulative %",
        yaxis="y2",
        mode="lines+markers",
        line=dict(color="orange", width=2),
        marker=dict(size=6),
    ))
    fig2.add_hline(
        y=80, yref="y2", line_dash="dash", line_color="red",
        annotation_text="80% threshold", annotation_position="right"
    )
    fig2.update_layout(
        title=f"Financial Variance by Field — {dataset}  (Total: ${total_variance:,.2f})",
        yaxis=dict(title="Variance ($)"),
        yaxis2=dict(title="Cumulative %", overlaying="y", side="right",
                    range=[0, 105], ticksuffix="%"),
        xaxis_tickangle=-35,
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        height=500,
    )
    st.plotly_chart(fig2, use_container_width=True)

    col_a, col_b = st.columns(2)
    col_a.metric("Total Financial Variance", f"${total_variance:,.2f}")
    n_fields_80 = int(query(f"""
        SELECT COUNT(*) AS n FROM {fin_view}
        WHERE RUNNING_PCT_OF_TOTAL <= 0.80
    """).iloc[0]["n"])
    col_b.metric("Fields driving 80% of variance",
                 f"{n_fields_80} of {len(df_fin)}")


# ── Tab 4: Discrepancy Drill-Down ─────────────────────────────────────────────

with tab4:
    st.subheader("Discrepancy Records")

    view_choice = st.selectbox("Select report", [
        "Missing Beneficiaries",
        "Missing Claims",
        "Beneficiary Attribute Mismatches",
        "Claim Payment Discrepancies",
    ])

    view_map = {
        "Missing Beneficiaries":          ("vw_beneficiary_errors",
                                           "finding = 'Error: Beneficiary Missing in New File'"),
        "Missing Claims":                  ("vw_claim_mismatches",                       None),
        "Beneficiary Attribute Mismatches":("vw_beneficiary_attribute_errors",           None),
        "Claim Payment Discrepancies":     ("vw_claim_line_nch_pmt_amt_1_differences",   None),
    }

    view_name, base_where = view_map[view_choice]
    where_clause = f"WHERE {base_where}" if base_where else ""

    df_disc = query(f"SELECT * FROM {view_name} {where_clause} LIMIT 1000")
    total_rows = int(query(f"SELECT COUNT(*) AS n FROM {view_name} {where_clause}").iloc[0]["n"])

    search = st.text_input("Filter (search across all columns)")
    if search:
        mask = df_disc.astype(str).apply(
            lambda col: col.str.contains(search, case=False, na=False)
        ).any(axis=1)
        df_disc = df_disc[mask]

    st.caption(f"{total_rows:,} total records · showing {len(df_disc):,}")
    st.dataframe(df_disc, hide_index=True, use_container_width=True)


# ── Tab 5: Row Count Validation ───────────────────────────────────────────────

with tab5:
    st.subheader("Row Count Validation — Source vs. New System")

    df_counts = query("SELECT * FROM vw_source_counts")
    df_counts.columns = [c.upper() for c in df_counts.columns]

    col_source = next((c for c in df_counts.columns if "SRC" in c or "SOURCE" in c), None)
    col_new    = next((c for c in df_counts.columns if c.startswith("NEW")), None)
    col_subj   = next((c for c in df_counts.columns
                       if c not in (col_source, col_new, "DIFF")), None)

    if col_source and col_new and col_subj:
        for _, row in df_counts.iterrows():
            delta = int(row["DIFF"]) if "DIFF" in df_counts.columns else int(row[col_new]) - int(row[col_source])
            pct   = delta / int(row[col_source]) * 100 if row[col_source] else 0
            icon  = "✅" if delta == 0 else ("⚠️" if abs(pct) < 1 else "🔴")
            st.markdown(f"#### {icon} {row[col_subj]}")
            c1, c2, c3 = st.columns(3)
            c1.metric("Source Count",     f"{int(row[col_source]):,}")
            c2.metric("New System Count", f"{int(row[col_new]):,}")
            c3.metric("Delta",            f"{delta:+,}",
                      delta_color="inverse" if delta != 0 else "off")

        st.divider()
        fig_bar = px.bar(
            df_counts.melt(id_vars=[col_subj], value_vars=[col_source, col_new],
                           var_name="System", value_name="Row Count"),
            x=col_subj,
            y="Row Count",
            color="System",
            barmode="group",
            title="Source vs. New System Row Counts",
            text_auto=True,
        )
        st.plotly_chart(fig_bar, use_container_width=True)
    else:
        st.dataframe(df_counts, hide_index=True, use_container_width=True)
        st.caption("Could not auto-detect column layout — raw view shown above.")


# ── Tab 6: Beneficiary Defect Heatmap ─────────────────────────────────────────

with tab6:
    st.subheader("Beneficiary Defect Heatmap — Errors per Field")

    df_audit_sample = query("SELECT * FROM audit_beneficiary_summary LIMIT 1")
    all_audit_cols  = [c.upper() for c in df_audit_sample.columns]
    non_flag_cols   = {"DESYNPUF_ID", "YEAR", "BENE_BIRTH_DT", "BENE_DEATH_DT",
                       "BENE_SEX_IDENT_CD", "BENE_RACE_CD", "BENE_ESRD_IND",
                       "SP_STATE_CODE", "BENE_COUNTY_CD", "BENE_HI_CVRAGE_TOT_MON",
                       "BENE_SMI_CVRAGE_TOT_MON", "BENE_HMO_CVRAGE_TOT_MON",
                       "PLAN_CVRG_MOS_NUM", "MEDREIMB_IP", "BENRES_IP",
                       "PPPYMT_IP", "MEDREIMB_OP", "BENRES_OP", "PPPYMT_OP",
                       "MEDREIMB_CAR", "BENRES_CAR", "PPPYMT_CAR"}
    flag_cols = [c for c in all_audit_cols if c not in non_flag_cols]

    if flag_cols:
        sum_sql = ", ".join([f'SUM("{c}") AS "{c}"' for c in flag_cols])
        df_sums = query(f"SELECT {sum_sql} FROM audit_beneficiary_summary")
        df_sums.columns = [c.upper() for c in df_sums.columns]

        df_melted = df_sums.T.reset_index()
        df_melted.columns = ["Field", "Error_Count"]
        df_melted = df_melted.sort_values("Error_Count", ascending=False)

        top_n_heat = st.slider("Show top N fields", 5, len(df_melted),
                               min(20, len(df_melted)), key="heat_slider")
        df_heat = df_melted.head(top_n_heat)

        fig_heat = px.bar(
            df_heat,
            x="Error_Count",
            y="Field",
            orientation="h",
            color="Error_Count",
            color_continuous_scale=["green", "orange", "red"],
            title=f"Beneficiary Fields by Total Error Count (top {top_n_heat})",
            labels={"Error_Count": "Total Errors", "Field": ""},
            text="Error_Count",
        )
        fig_heat.update_traces(textposition="outside")
        fig_heat.update_layout(coloraxis_showscale=False,
                               height=max(400, top_n_heat * 28))
        st.plotly_chart(fig_heat, use_container_width=True)

        with st.expander("Full error count table"):
            st.dataframe(df_melted.reset_index(drop=True),
                         hide_index=True, use_container_width=True)
    else:
        st.info("No flag columns detected in audit_beneficiary_summary.")


# ── Tab 7: Chronic Condition Error Analysis ────────────────────────────────────

with tab7:
    st.subheader("Chronic Condition Flags — Migration Error Rates")

    sp_cols_sql = (
        'SUM("SP_ALZHDMTA") AS SP_ALZHDMTA, '
        'SUM("SP_CHF") AS SP_CHF, '
        'SUM("SP_CHRNKIDN") AS SP_CHRNKIDN, '
        'SUM("SP_CNCR") AS SP_CNCR, '
        'SUM("SP_COPD") AS SP_COPD, '
        'SUM("SP_DEPRESSN") AS SP_DEPRESSN, '
        'SUM("SP_DIABETES") AS SP_DIABETES, '
        'SUM("SP_ISCHMCHT") AS SP_ISCHMCHT, '
        'SUM("SP_OSTEOPRS") AS SP_OSTEOPRS, '
        'SUM("SP_RA_OA") AS SP_RA_OA, '
        'SUM("SP_STRKETIA") AS SP_STRKETIA, '
        'COUNT(*) AS TOTAL_BENES'
    )
    df_sp = query(f"SELECT {sp_cols_sql} FROM audit_beneficiary_summary")
    df_sp.columns = [c.upper() for c in df_sp.columns]

    total_benes = int(df_sp["TOTAL_BENES"].iloc[0])

    condition_map = {
        "SP_ALZHDMTA": "Alzheimer's / Dementia",
        "SP_CHF":      "Congestive Heart Failure",
        "SP_CHRNKIDN": "Chronic Kidney Disease",
        "SP_CNCR":     "Cancer",
        "SP_COPD":     "COPD",
        "SP_DEPRESSN": "Depression",
        "SP_DIABETES": "Diabetes",
        "SP_ISCHMCHT": "Ischemic Heart Disease",
        "SP_OSTEOPRS": "Osteoporosis",
        "SP_RA_OA":    "Rheumatoid / Osteoarthritis",
        "SP_STRKETIA": "Stroke / TIA",
    }

    import pandas as pd
    cond_rows = []
    for col, label in condition_map.items():
        if col in df_sp.columns:
            errors = int(df_sp[col].iloc[0])
            cond_rows.append({
                "Condition":   label,
                "Error_Count": errors,
                "Error_Rate":  errors / total_benes if total_benes else 0,
            })

    df_conditions = pd.DataFrame(cond_rows).sort_values("Error_Count", ascending=False)

    fig_cond = px.bar(
        df_conditions,
        x="Error_Count",
        y="Condition",
        orientation="h",
        color="Error_Rate",
        color_continuous_scale=["green", "orange", "red"],
        title=f"Migration Errors by Chronic Condition Flag  (n={total_benes:,} beneficiaries)",
        labels={"Error_Count": "Errors", "Error_Rate": "Error Rate"},
        text=df_conditions["Error_Rate"].apply(lambda r: f"{r*100:.2f}%"),
    )
    fig_cond.update_traces(textposition="outside")
    fig_cond.update_layout(coloraxis_showscale=True,
                           coloraxis_colorbar_tickformat=".1%",
                           height=460)
    st.plotly_chart(fig_cond, use_container_width=True)

    st.dataframe(
        df_conditions.assign(
            Error_Rate=df_conditions["Error_Rate"].map("{:.4%}".format)
        ).reset_index(drop=True),
        hide_index=True,
        use_container_width=True,
    )


# ── Tab 8: Year-over-Year Quality Trend ───────────────────────────────────────

with tab8:
    st.subheader("Year-over-Year Migration Error Trend")

    df_yoy_sample = query("SELECT * FROM audit_beneficiary_summary LIMIT 1")
    yoy_non_flag  = {"DESYNPUF_ID", "YEAR", "BENE_BIRTH_DT", "BENE_DEATH_DT",
                     "BENE_SEX_IDENT_CD", "BENE_RACE_CD", "BENE_ESRD_IND",
                     "SP_STATE_CODE", "BENE_COUNTY_CD", "BENE_HI_CVRAGE_TOT_MON",
                     "BENE_SMI_CVRAGE_TOT_MON", "BENE_HMO_CVRAGE_TOT_MON",
                     "PLAN_CVRG_MOS_NUM", "MEDREIMB_IP", "BENRES_IP",
                     "PPPYMT_IP", "MEDREIMB_OP", "BENRES_OP", "PPPYMT_OP",
                     "MEDREIMB_CAR", "BENRES_CAR", "PPPYMT_CAR"}
    yoy_flag_cols = [c.upper() for c in df_yoy_sample.columns
                     if c.upper() not in yoy_non_flag]

    if yoy_flag_cols:
        sum_exprs = ", ".join([f'SUM("{c}") AS "{c}"' for c in yoy_flag_cols])
        df_yoy = query(f"""
            SELECT "YEAR",
                   {sum_exprs},
                   COUNT(*) AS BENE_COUNT
            FROM audit_beneficiary_summary
            GROUP BY "YEAR"
            ORDER BY "YEAR"
        """)
        df_yoy.columns = [c.upper() for c in df_yoy.columns]

        valid_flags = [c for c in yoy_flag_cols if c in df_yoy.columns]
        df_yoy["TOTAL_ERRORS"] = df_yoy[valid_flags].sum(axis=1)
        df_yoy["ERROR_RATE"]   = df_yoy["TOTAL_ERRORS"] / df_yoy["BENE_COUNT"]

        fig_yoy = go.Figure()
        fig_yoy.add_trace(go.Bar(
            x=df_yoy["YEAR"].astype(str),
            y=df_yoy["TOTAL_ERRORS"],
            name="Total Errors",
            marker_color="#1f77b4",
            text=df_yoy["TOTAL_ERRORS"].apply(lambda v: f"{v:,.0f}"),
            textposition="outside",
        ))
        fig_yoy.add_trace(go.Scatter(
            x=df_yoy["YEAR"].astype(str),
            y=df_yoy["ERROR_RATE"] * 100,
            name="Error Rate %",
            yaxis="y2",
            mode="lines+markers",
            line=dict(color="orange", width=2),
            marker=dict(size=8),
        ))
        fig_yoy.update_layout(
            title="Beneficiary Migration Errors by Year",
            yaxis=dict(title="Total Errors"),
            yaxis2=dict(title="Error Rate %", overlaying="y", side="right",
                        ticksuffix="%"),
            xaxis_title="Year",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            height=450,
        )
        st.plotly_chart(fig_yoy, use_container_width=True)

        st.dataframe(
            df_yoy[["YEAR", "BENE_COUNT", "TOTAL_ERRORS", "ERROR_RATE"]]
                .assign(ERROR_RATE=df_yoy["ERROR_RATE"].map("{:.4%}".format))
                .reset_index(drop=True),
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.info("No flag columns found in audit_beneficiary_summary.")


# ── Tab 9: Go / No-Go Executive Summary ───────────────────────────────────────

with tab9:
    st.subheader("Go / No-Go Executive Summary")
    st.caption("Traffic-light readiness assessment by Six Sigma threshold")

    def traffic_light(sigma: float) -> tuple:
        if sigma >= 4.5:
            return "🟢 GO",      "Meets target (≥ 4.5σ)"
        elif sigma >= 4.0:
            return "🟡 CAUTION", "Acceptable but below target (4.0–4.5σ)"
        else:
            return "🔴 NO-GO",  "Below minimum threshold (< 4.0σ)"

    st.markdown("### Overall Migration Readiness")
    col_c, col_b = st.columns(2)

    for col, row, label in [
        (col_c, carrier_row, "Carrier Claims"),
        (col_b, bene_row,    "Beneficiary Summary"),
    ]:
        verdict, explanation = traffic_light(row["SIGMA_LEVEL"])
        with col:
            st.markdown(f"#### {verdict} — {label}")
            st.markdown(f"**Sigma Level:** {row['SIGMA_LEVEL']:.3f}")
            st.markdown(f"**DPMO:** {row['DPMO']:,.0f}")
            st.markdown(f"**Yield:** {row['YIELD'] * 100:.4f}%")
            st.caption(explanation)

    st.divider()
    st.markdown("### Field-Level Readiness — Carrier Claims")

    df_carrier_go = query("""
        SELECT FIELD_FAMILY, SIGMA_LEVEL, DPMO, TOTAL_DEFECTS
        FROM vw_sigma_analysis_carrier_columns
        ORDER BY SIGMA_LEVEL
    """)
    df_carrier_go["STATUS"] = df_carrier_go["SIGMA_LEVEL"].apply(
        lambda s: "🟢 GO" if s >= 4.5 else ("🟡 CAUTION" if s >= 4.0 else "🔴 NO-GO")
    )

    go_c      = (df_carrier_go["SIGMA_LEVEL"] >= 4.5).sum()
    caution_c = ((df_carrier_go["SIGMA_LEVEL"] >= 4.0) &
                 (df_carrier_go["SIGMA_LEVEL"] < 4.5)).sum()
    nogo_c    = (df_carrier_go["SIGMA_LEVEL"] < 4.0).sum()

    mc1, mc2, mc3 = st.columns(3)
    mc1.metric("🟢 GO fields",      int(go_c))
    mc2.metric("🟡 CAUTION fields", int(caution_c))
    mc3.metric("🔴 NO-GO fields",   int(nogo_c))

    with st.expander("Carrier Claims — field detail"):
        st.dataframe(df_carrier_go, hide_index=True, use_container_width=True)

    st.markdown("### Field-Level Readiness — Beneficiary Summary")

    df_bene_go = query("""
        SELECT FIELD_FAMILY, SIGMA_LEVEL, DPMO, TOTAL_DEFECTS
        FROM vw_sigma_analysis_beneficiary_columns
        ORDER BY SIGMA_LEVEL
    """)
    df_bene_go["STATUS"] = df_bene_go["SIGMA_LEVEL"].apply(
        lambda s: "🟢 GO" if s >= 4.5 else ("🟡 CAUTION" if s >= 4.0 else "🔴 NO-GO")
    )

    go_b      = (df_bene_go["SIGMA_LEVEL"] >= 4.5).sum()
    caution_b = ((df_bene_go["SIGMA_LEVEL"] >= 4.0) &
                 (df_bene_go["SIGMA_LEVEL"] < 4.5)).sum()
    nogo_b    = (df_bene_go["SIGMA_LEVEL"] < 4.0).sum()

    mb1, mb2, mb3 = st.columns(3)
    mb1.metric("🟢 GO fields",      int(go_b))
    mb2.metric("🟡 CAUTION fields", int(caution_b))
    mb3.metric("🔴 NO-GO fields",   int(nogo_b))

    with st.expander("Beneficiary Summary — field detail"):
        st.dataframe(df_bene_go, hide_index=True, use_container_width=True)

    st.divider()
    st.markdown("### Threshold Reference")
    st.dataframe(
        query("""
            SELECT * FROM (VALUES
                ('≥ 4.5σ',  '🟢 GO',      'Meets target — ready for production'),
                ('4.0–4.5σ','🟡 CAUTION', 'Acceptable — monitor closely post-migration'),
                ('< 4.0σ',  '🔴 NO-GO',   'Remediation required before go-live')
            ) AS t(threshold, status, meaning)
        """),
        hide_index=True,
        use_container_width=True,
    )
