# app.py
# CSE Media Monitoring — Polished / Professional UI (NO sidebar)
# This version includes:
# ✅ Filter panel: time filters row + OTHER filters split into 2 rows
#    Row A: Sort, Topic (with All), Geographic Scope, Media
#    Row B: Factchecking Site, Comments, Format, Election Related
# ✅ Search moved to "Filtered Articles Feed" section (feed-only search)
# ✅ KPI row = 4 cards: Total Articles, Unique Articles (Sort=unique), Distinct Topics, Fact-checking Sites
# ✅ Topic line chart: default shows Top 5 topics when Topic=["All"]; shows all selected topics when user selects specific ones
# ✅ Media/Format/Scope tabs: Pie vs Line + optional numbers tables (default OFF)
# ✅ Unique Streamlit keys for buttons; no duplicate-key crashes

import math
import hashlib
import pandas as pd
import streamlit as st
import plotly.express as px
import gspread
from google.oauth2.service_account import Credentials

# ======================================================
# PAGE CONFIG
# ======================================================
st.set_page_config(page_title="Fact-Check Monitoring & Analytics", layout="wide")

# ======================================================
# LIGHT CSS POLISH
# ======================================================
st.markdown(
    """
    <style>
      .block-container { padding-top: 1.0rem; padding-bottom: 2.0rem; }
      [data-testid="stMetric"] { padding: 10px 12px; border-radius: 14px; }
      [data-testid="stExpander"] { border-radius: 14px; overflow: hidden; }
      [data-testid="stExpander"] > details { border-radius: 14px; }
      .stContainer { border-radius: 14px; }
      div[data-baseweb="select"] > div { border-radius: 12px; }
      .js-plotly-plot { margin-bottom: -10px; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ======================================================
# CONFIG
# ======================================================
SPREADSHEET_ID = st.secrets.get("SPREADSHEET_ID", "")
SHEET_TABS = st.secrets.get("SHEET_TABS", ["Sheet3"])
TIMEZONE_LABEL = "Dhaka"

# Column names in your Google Sheet
COL_DATE = "Date"
COL_HEADLINE = "Headline"
COL_LINK = "Link"
COL_SITE = "Factchecking Site"
COL_TOPIC = "Topic"
COL_ELECTION = "Election Related"
COL_SORT = "Sort"
COL_SCOPE = "Geographic Scope"
COL_MEDIA = "Media"
COL_FORMAT = "Format"
COL_COMMENTS = "Comments"
COL_CLAIM = "Claim"
COL_FULL = "Full Content"

# Optional columns (won't break if missing)
COL_PERSON = "Person"
COL_ORG = "Organization"

# ======================================================
# GOOGLE SHEETS
# ======================================================
def _get_gspread_client():
    creds_info = dict(st.secrets["GSHEETS_SA"])
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)

@st.cache_data(ttl=300, show_spinner=False)
def load_sheet_tabs(spreadsheet_id: str, tabs: list[str]) -> pd.DataFrame:
    gc = _get_gspread_client()
    sh = gc.open_by_key(spreadsheet_id)

    frames = []
    for tab in tabs:
        try:
            ws = sh.worksheet(tab)
        except Exception:
            st.warning(f"Sheet tab not found (skipped): {tab}")
            continue

        values = ws.get_all_values()
        if not values or len(values) < 2:
            continue

        header = values[0]
        rows = values[1:]
        dfx = pd.DataFrame(rows, columns=header)
        dfx["__sheet_tab"] = tab
        frames.append(dfx)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)

# ======================================================
# UTIL
# ======================================================
def to_datetime_safe(s: pd.Series) -> pd.Series:
    dt = pd.to_datetime(s, errors="coerce")
    if dt.isna().mean() > 0.5:
        dt2 = pd.to_datetime(s, errors="coerce", dayfirst=True)
        if dt2.isna().mean() < dt.isna().mean():
            dt = dt2
    return dt

def normalize_yes_no(val: str) -> str:
    if val is None:
        return ""
    v = str(val).strip().lower()
    if v in {"yes", "y", "true", "1", "হ্যাঁ"}:
        return "Yes"
    if v in {"no", "n", "false", "0", "না"}:
        return "No"
    return str(val).strip()

def as_str(x) -> str:
    return "" if x is None else str(x)

def apply_multiselect_filter(df: pd.DataFrame, col: str, selected: list[str]) -> pd.DataFrame:
    if not selected:
        return df
    return df[df[col].astype(str).isin(selected)]

def options_from_series(series: pd.Series):
    return sorted([x for x in series.dropna().astype(str).unique().tolist() if x.strip() != ""])

def safe_value_counts(series: pd.Series):
    return series.astype(str).replace("", pd.NA).dropna().value_counts()

def make_chip(label: str, value: str) -> str:
    return (
        "<span style='display:inline-block;padding:4px 10px;border-radius:999px;"
        "border:1px solid #E5E7EB;background:#F9FAFB;margin:3px 6px 0 0;font-size:0.85rem;'>"
        f"<b>{label}:</b> {value}</span>"
    )

def reset_filter_state():
    keys = list(st.session_state.keys())
    for k in keys:
        if k in {"page", "feed_search_prev"}:
            continue
        if k in {"preset", "year", "quarter", "month", "gran", "date_range"}:
            st.session_state.pop(k, None)
            continue
        if k.startswith("f_"):
            st.session_state.pop(k, None)

def compute_bucket(dts: pd.Series, gran: str) -> pd.Series:
    if gran == "Daily":
        return dts.dt.to_period("D").astype(str)
    if gran == "Weekly":
        return dts.dt.to_period("W").astype(str)
    if gran == "Monthly":
        return dts.dt.to_period("M").astype(str)
    if gran == "Quarterly":
        return dts.dt.to_period("Q").astype(str)
    return dts.dt.to_period("Y").astype(str)

def donut_for(col: str, title: str, max_slices: int = 8):
    """Return (fig, table_df) for a donut distribution chart on df_f[col]."""
    global df_f
    if df_f is None or df_f.empty or col not in df_f.columns:
        return None, pd.DataFrame()
    s = df_f[col].dropna().astype(str).str.strip()
    s = s[s != ""]
    if s.empty:
        return None, pd.DataFrame()
    vc = s.value_counts().rename("Articles").reset_index().rename(columns={"index": col})
    # keep top N, bucket rest into Others to avoid unreadable legends
    if len(vc) > max_slices:
        top = vc.head(max_slices)
        others = pd.DataFrame({col: ["Others"], "Articles": [int(vc["Articles"].iloc[max_slices:].sum())]})
        vc = pd.concat([top, others], ignore_index=True)
    fig = px.pie(vc, names=col, values="Articles", hole=0.5, title=None)
    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), height=420, legend_title_text=col)
    tbl = vc.copy()
    total = int(tbl["Articles"].sum()) if not tbl.empty else 0
    tbl["Share %"] = (tbl["Articles"] / max(1, total) * 100).round(2)
    return fig, tbl


def time_series_for(col: str, title: str):
    """Return (fig_line, grp_df) where grp_df has columns: bucket, col, Articles."""
    global df_f, granularity
    if df_f is None or df_f.empty or COL_DATE not in df_f.columns or col not in df_f.columns:
        return None, pd.DataFrame(columns=["bucket", col, "Articles"])
    df_time = df_f.dropna(subset=[COL_DATE]).copy()
    if df_time.empty:
        return None, pd.DataFrame(columns=["bucket", col, "Articles"])
    df_time["bucket"] = compute_bucket(df_time[COL_DATE], granularity)
    df_time[col] = df_time[col].astype(str).str.strip()
    df_time = df_time[df_time[col] != ""]
    if df_time.empty:
        return None, pd.DataFrame(columns=["bucket", col, "Articles"])
    grp = (
        df_time.groupby(["bucket", col], dropna=False)
        .size()
        .reset_index(name="Articles")
        .sort_values("bucket")
    )
    fig = px.line(grp, x="bucket", y="Articles", color=col, markers=True)
    fig.update_layout(
        margin=dict(l=10, r=10, t=10, b=10),
        legend_title_text=col,
        xaxis_title="Time",
        yaxis_title="Articles",
        height=420,
    )
    return fig, grp


def time_series_pct_from_grp(col: str, title: str, grp: pd.DataFrame):
    """Given grp(bucket, col, Articles), return (fig_pct, df_pct)."""
    if grp is None or grp.empty:
        return None, pd.DataFrame(columns=["bucket", col, "Articles", "Total", "Percent"])
    totals = grp.groupby("bucket", as_index=False)["Articles"].sum().rename(columns={"Articles": "Total"})
    df_pct = grp.merge(totals, on="bucket", how="left")
    df_pct["Percent"] = (df_pct["Articles"] / df_pct["Total"] * 100).fillna(0)
    fig = px.line(df_pct, x="bucket", y="Percent", color=col, markers=True)
    fig.update_layout(
        margin=dict(l=10, r=10, t=10, b=10),
        legend_title_text=col,
        xaxis_title="Time",
        yaxis_title="Share of articles (%)",
        height=420,
    )
    fig.update_yaxes(range=[0, 100])
    return fig, df_pct

# ======================================================
# HEADER BAR
# ======================================================
hdr = st.container(border=True)
with hdr:
    c1, c2, c3 = st.columns([0.65, 0.2, 0.15], vertical_alignment="center")
    with c1:
        st.markdown("## Fact-Check Monitoring & Analytics")
        st.caption("Fact-check Article Dashboard")
    with c2:
        st.markdown("**Connected:** ✅")
        st.caption(f"Timezone: {TIMEZONE_LABEL}")
    with c3:
        if st.button("Refresh", width="stretch"):
            st.cache_data.clear()
            st.rerun()

# ======================================================
# LOAD DATA
# ======================================================
with st.spinner("Loading data from Google Sheets..."):
    df_raw = load_sheet_tabs(SPREADSHEET_ID, SHEET_TABS) if SPREADSHEET_ID else pd.DataFrame()

if df_raw.empty:
    st.error("No data loaded. Check SPREADSHEET_ID / SHEET_TABS and Google sharing permissions.")
    st.stop()

# Ensure columns exist
for col in [
    COL_DATE, COL_HEADLINE, COL_LINK, COL_SITE, COL_TOPIC, COL_ELECTION, COL_SORT,
    COL_SCOPE, COL_MEDIA, COL_FORMAT, COL_COMMENTS, COL_CLAIM, COL_FULL,
    COL_PERSON, COL_ORG
]:
    if col not in df_raw.columns:
        df_raw[col] = ""

df = df_raw.copy()

# Parse + normalize
df[COL_DATE] = to_datetime_safe(df[COL_DATE])
df["Year"] = df[COL_DATE].dt.year
df["MonthName"] = df[COL_DATE].dt.strftime("%b")
df["Quarter"] = df[COL_DATE].dt.to_period("Q").astype(str)

df[COL_ELECTION] = df[COL_ELECTION].apply(normalize_yes_no)
df[COL_SORT] = df[COL_SORT].astype(str).str.strip().str.lower()  # normalize sort to unique/repeat

min_date = df[COL_DATE].min()
max_date = df[COL_DATE].max()
years = sorted([int(y) for y in df["Year"].dropna().unique().tolist()])

# ======================================================
# FILTER PANEL (TOP) — NO SIDEBAR
# ======================================================
filter_panel = st.expander("Filters", expanded=True)
with filter_panel:
    topbar = st.columns([0.75, 0.25], vertical_alignment="center")
    with topbar[0]:
        st.caption("Tip: Use presets for quick navigation, then refine with filters.")
    with topbar[1]:
        if st.button("Reset filters", width="stretch"):
            reset_filter_state()
            st.session_state.page = 1
            st.rerun()

    # Row 1: time-related filters
    r1 = st.columns([1.6, 1.05, 1.25, 1.25, 1.25, 1.7], vertical_alignment="bottom")

    with r1[0]:
        preset = st.radio(
            "Time preset",
            ["All Time", "This Year", "This Quarter", "This Month", "Custom Range"],
            horizontal=True,
            index=0,
            key="preset",
        )

    with r1[1]:
        default_year = max(years) if years else None
        year_sel = st.selectbox("Year", options=["(Any)"] + years, index=(1 if default_year else 0), key="year")

    with r1[2]:
        quarters = sorted([q for q in df["Quarter"].dropna().unique().tolist()])
        quarter_sel = st.multiselect("Quarter", quarters, default=[], key="quarter")

    with r1[3]:
        month_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
        month_sel = st.multiselect("Month", month_names, default=[], key="month")

    with r1[4]:
        granularity = st.selectbox(
            "Line chart granularity",
            ["Daily", "Weekly", "Monthly", "Quarterly", "Yearly"],
            index=2,  # Monthly default
            key="gran",
        )

    with r1[5]:
        start_default = min_date.date() if pd.notna(min_date) else None
        end_default = max_date.date() if pd.notna(max_date) else None
        date_range = st.date_input("Custom date range", value=(start_default, end_default), key="date_range")

    # Apply time filters
    df_f = df.copy()

    if preset == "This Year" and pd.notna(max_date):
        df_f = df_f[df_f[COL_DATE].dt.year == int(max_date.year)]
    elif preset == "This Quarter" and pd.notna(max_date):
        current_q = pd.Period(max_date, freq="Q")
        df_f = df_f[df_f[COL_DATE].dt.to_period("Q") == current_q]
    elif preset == "This Month" and pd.notna(max_date):
        df_f = df_f[
            (df_f[COL_DATE].dt.year == int(max_date.year))
            & (df_f[COL_DATE].dt.month == int(max_date.month))
        ]
    elif preset == "Custom Range":
        if isinstance(date_range, tuple) and len(date_range) == 2 and all(date_range):
            start, end = date_range
            df_f = df_f[
                (df_f[COL_DATE] >= pd.Timestamp(start))
                & (df_f[COL_DATE] <= pd.Timestamp(end) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1))
            ]

    if year_sel != "(Any)":
        df_f = df_f[df_f["Year"] == int(year_sel)]
    if quarter_sel:
        df_f = df_f[df_f["Quarter"].astype(str).isin([str(q) for q in quarter_sel])]
    if month_sel:
        df_f = df_f[df_f["MonthName"].astype(str).isin(month_sel)]

    # Row 2A: Sort, Topic, Geographic Scope, Media
    r2a = st.columns([1.15, 2.0, 1.6, 1.35], vertical_alignment="bottom")

    with r2a[0]:
        sel_sort = st.multiselect("Sort", options_from_series(df_f[COL_SORT]), default=[], key="f_sort")

    with r2a[1]:
        topic_opts = options_from_series(df_f[COL_TOPIC])
        topic_opts = ["All"] + topic_opts
        sel_topic = st.multiselect("Topic", topic_opts, default=[], key="f_topic")

    with r2a[2]:
        sel_scope = st.multiselect("Geographic Scope", options_from_series(df_f[COL_SCOPE]), default=[], key="f_scope")

    with r2a[3]:
        sel_media = st.multiselect("Media", options_from_series(df_f[COL_MEDIA]), default=[], key="f_media")

    # Row 2B: Factchecking Site, Comments, Format, Election Related
    r2b = st.columns([1.7, 1.35, 1.35, 1.25], vertical_alignment="bottom")

    with r2b[0]:
        sel_site = st.multiselect("Factchecking Site", options_from_series(df_f[COL_SITE]), default=[], key="f_site")

    with r2b[1]:
        sel_comments = st.multiselect("Comments", options_from_series(df_f[COL_COMMENTS]), default=[], key="f_comments")

    with r2b[2]:
        sel_format = st.multiselect("Format", options_from_series(df_f[COL_FORMAT]), default=[], key="f_format")

    with r2b[3]:
        sel_election = st.multiselect("Election Related", ["Yes", "No"], default=[], key="f_election")

    # Apply category filters
    df_f = apply_multiselect_filter(df_f, COL_SORT, sel_sort)
    df_f = apply_multiselect_filter(df_f, COL_SCOPE, sel_scope)
    df_f = apply_multiselect_filter(df_f, COL_MEDIA, sel_media)
    df_f = apply_multiselect_filter(df_f, COL_SITE, sel_site)

    # Topic filter only if user removed "All"
    if sel_topic and "All" not in sel_topic:
        df_f = df_f[df_f[COL_TOPIC].astype(str).isin(sel_topic)]

    df_f = apply_multiselect_filter(df_f, COL_COMMENTS, sel_comments)
    df_f = apply_multiselect_filter(df_f, COL_FORMAT, sel_format)
    df_f = apply_multiselect_filter(df_f, COL_ELECTION, sel_election)

# ======================================================
# ACTIVE FILTER CHIPS (Search removed from here)
# ======================================================
chips = []
if preset != "All Time":
    chips.append(make_chip("Preset", preset))
if year_sel != "(Any)":
    chips.append(make_chip("Year", str(year_sel)))
if quarter_sel:
    chips.append(make_chip("Quarter", ", ".join(quarter_sel)))
if month_sel:
    chips.append(make_chip("Month", ", ".join(month_sel)))
chips.append(make_chip("Granularity", granularity))

def add_multi_chip(label, vals):
    if vals:
        chips.append(make_chip(label, ", ".join(vals)))

add_multi_chip("Sort", sel_sort)
add_multi_chip("Scope", sel_scope)
add_multi_chip("Media", sel_media)
add_multi_chip("Site", sel_site)
chips.append(make_chip("Topic", "All" if sel_topic == ["All"] else ", ".join(sel_topic)))
add_multi_chip("Comments", sel_comments)
add_multi_chip("Format", sel_format)
add_multi_chip("Election", sel_election)

chip_box = st.container(border=True)
with chip_box:
    st.markdown("**Active filters**")
    if chips:
        st.markdown("".join(chips), unsafe_allow_html=True)
    else:
        st.caption("None (showing all data).")

# ======================================================
# KPI (4 cards)
# ======================================================
kpi = st.container(border=True)
with kpi:
    a, b, c, d = st.columns(4, vertical_alignment="center")

    total_articles = int(len(df_f))
    unique_articles = int((df_f[COL_SORT].astype(str).str.strip().str.lower() == "unique").sum())
    topics_count = int(df_f[COL_TOPIC].replace("", pd.NA).dropna().nunique())
    sites_count = int(df_f[COL_SITE].replace("", pd.NA).dropna().nunique())

    top_topic = safe_value_counts(df_f[COL_TOPIC]).head(1)
    top_site = safe_value_counts(df_f[COL_SITE]).head(1)

    with a:
        st.metric("Total Articles", total_articles)
        if pd.notna(min_date) and pd.notna(max_date):
            st.caption(f"Range: {min_date.date()} → {max_date.date()}")

    with b:
        st.metric("Unique Articles", unique_articles)
        repeats = total_articles - unique_articles
        if total_articles > 0:
            st.caption(f"Repeat rows: {repeats} ({(repeats/total_articles)*100:.1f}%)")
        else:
            st.caption("Repeat rows: 0")

    with c:
        st.metric("Distinct Topics", topics_count)
        if not top_topic.empty:
            share = (top_topic.iloc[0] / max(1, total_articles)) * 100
            st.caption(f"Top topic: {top_topic.index[0]} ({share:.1f}%)")

    with d:
        st.metric("Fact-checking Sites", sites_count)
        if not top_site.empty:
            st.caption(f"Top site: {top_site.index[0]} ({int(top_site.iloc[0])})")

# ======================================================
# TOPIC CHARTS (Line + Pie)
# Line chart behavior:
#   - Default Topic=["All"]: show TOP 5 topics
#   - If user selected topics: show ALL selected topics
# ======================================================
topic_row = st.container(border=True)
with topic_row:
    left, right = st.columns([0.68, 0.32], gap="large", vertical_alignment="top")

    df_time = df_f.dropna(subset=[COL_DATE]).copy()
    df_time["bucket"] = compute_bucket(df_time[COL_DATE], granularity)

    topic_counts = safe_value_counts(df_time[COL_TOPIC])

    if not sel_topic:
        chart_topics = topic_counts.head(5).index.tolist()
        chart_label = "Showing: Top 5 topics"
    elif "All" in sel_topic:
        chart_topics = topic_counts.index.tolist()
        chart_label = "Showing: All topics"
    else:
        chart_topics = sel_topic
        chart_label = f"Showing: {len(chart_topics)} selected topics"

    with left:
        st.subheader("Topic Volume Over Time")
        st.caption(chart_label)

        if df_time.empty or not chart_topics:
            st.info("Not enough data for the topic-over-time chart with current filters.")
        else:
            tab_vol, tab_pct = st.tabs(["Volume", "% of total"])

            # -----------------------
            # Tab 1: Raw volume
            # -----------------------
            with tab_vol:
                df_line = (
                    df_time[df_time[COL_TOPIC].astype(str).isin(chart_topics)]
                    .groupby(["bucket", COL_TOPIC], as_index=False)
                    .size()
                    .rename(columns={"size": "Articles"})
                    .sort_values("bucket")
                )

                fig_line = px.line(df_line, x="bucket", y="Articles", color=COL_TOPIC, markers=True)
                fig_line.update_layout(
                    margin=dict(l=10, r=10, t=10, b=10),
                    legend_title_text="Topic",
                    xaxis_title="Time",
                    yaxis_title="Articles",
                    height=420,
                )
                st.plotly_chart(fig_line, width="stretch")

                if st.checkbox("Show numbers table", value=False, key="tbl_line_volume"):
                    pivot = (
                        df_line.pivot(index="bucket", columns=COL_TOPIC, values="Articles")
                        .fillna(0).astype(int).reset_index()
                        .rename(columns={"bucket": "Time"})
                    )
                    st.dataframe(pivot, width="stretch", hide_index=True, height=240)

            # -----------------------
            # Tab 2: % of total volume per time bucket
            # Denominator = ALL filtered articles in the bucket (not just selected topics)
            # -----------------------
            with tab_pct:
                df_topic = (
                    df_time[df_time[COL_TOPIC].astype(str).isin(chart_topics)]
                    .groupby(["bucket", COL_TOPIC], as_index=False)
                    .size()
                    .rename(columns={"size": "Articles"})
                    .sort_values("bucket")
                )

                df_totals = (
                    df_time.groupby("bucket", as_index=False)
                    .size()
                    .rename(columns={"size": "Total"})
                    .sort_values("bucket")
                )

                df_pct = df_topic.merge(df_totals, on="bucket", how="left")
                df_pct["Percent"] = (df_pct["Articles"] / df_pct["Total"] * 100).fillna(0)

                fig_pct = px.line(df_pct, x="bucket", y="Percent", color=COL_TOPIC, markers=True)
                fig_pct.update_layout(
                    margin=dict(l=10, r=10, t=10, b=10),
                    legend_title_text="Topic",
                    xaxis_title="Time",
                    yaxis_title="Share of articles (%)",
                    height=420,
                )
                fig_pct.update_yaxes(range=[0, 100])
                st.plotly_chart(fig_pct, width="stretch")

                if st.checkbox("Show numbers table", value=False, key="tbl_line_pct"):
                    pivot = (
                        df_pct.pivot(index="bucket", columns=COL_TOPIC, values="Percent")
                        .fillna(0).round(1).reset_index()
                        .rename(columns={"bucket": "Time"})
                    )
                    st.dataframe(pivot, width="stretch", hide_index=True, height=240)

    with right:
        st.subheader("Topic Distribution")

        topic_dist = safe_value_counts(df_f[COL_TOPIC]).reset_index()
        topic_dist.columns = [COL_TOPIC, "Articles"]

        if topic_dist.empty:
            st.info("No topic data.")
        else:
            if len(topic_dist) > 9:
                top = topic_dist.head(8)
                others = pd.DataFrame({COL_TOPIC: ["Others"], "Articles": [topic_dist["Articles"].iloc[8:].sum()]})
                topic_dist = pd.concat([top, others], ignore_index=True)

            fig_topic = px.pie(topic_dist, names=COL_TOPIC, values="Articles", hole=0.5)
            fig_topic.update_layout(margin=dict(l=10, r=10, t=10, b=10), height=420)
            st.plotly_chart(fig_topic, width="stretch")

            if st.checkbox("Show numbers table", value=False, key="tbl_topic_pie"):
                tbl = topic_dist.copy()
                total = int(tbl["Articles"].sum()) if not tbl.empty else 0
                tbl["Share %"] = (tbl["Articles"] / max(1, total) * 100).round(2)
                st.dataframe(tbl, width="stretch", hide_index=True, height=240)

# ======================================================
# MEDIA / FORMAT / SCOPE
# ======================================================
dist_row = st.container(border=True)
with dist_row:
    st.markdown("### Media • Format • Geographic Scope")
    p1, p2, p3 = st.columns(3, vertical_alignment="top")

    # MEDIA
    with p1:
        st.markdown("**Media**")
        tab_dist, tab_vol, tab_pct = st.tabs(["Distribution", "Over time (volume)", "Over time (% of total)"])

        # Precompute over-time series once (used by both volume and % tabs)
        fig_line, grp = time_series_for(COL_MEDIA, "Media Volume Over Time")
        fig_pct, grp_pct = time_series_pct_from_grp(COL_MEDIA, "Media % of Total Over Time", grp)

        with tab_dist:
            fig, tbl = donut_for(COL_MEDIA, "Media Distribution")
            if fig is None:
                st.info("No media data.")
            else:
                st.plotly_chart(fig, width="stretch")
                if st.checkbox("Show numbers table", value=False, key="tbl_media_pie"):
                    st.dataframe(tbl, width="stretch", hide_index=True, height=240)

        with tab_vol:
            if fig_line is None:
                st.info("Not enough data for media over time.")
            else:
                st.plotly_chart(fig_line, width="stretch")
                if st.checkbox("Show numbers table", value=False, key="tbl_media_line"):
                    pivot = (
                        grp.pivot(index="bucket", columns=COL_MEDIA, values="Articles")
                        .fillna(0).astype(int).reset_index()
                        .rename(columns={"bucket": "Time"})
                    )
                    st.dataframe(pivot, width="stretch", hide_index=True, height=240)

        with tab_pct:
            if fig_pct is None:
                st.info("Not enough data for media % over time.")
            else:
                st.plotly_chart(fig_pct, width="stretch")
                if st.checkbox("Show numbers table", value=False, key="tbl_media_pct"):
                    pivot = (
                        grp_pct.pivot(index="bucket", columns=COL_MEDIA, values="Percent")
                        .fillna(0).reset_index()
                        .rename(columns={"bucket": "Time"})
                    )
                    st.dataframe(pivot, width="stretch", hide_index=True, height=240)

    # FORMAT
    with p2:
        st.markdown("**Format**")
        tab_dist, tab_vol, tab_pct = st.tabs(["Distribution", "Over time (volume)", "Over time (% of total)"])

        fig_line, grp = time_series_for(COL_FORMAT, "Format Volume Over Time")
        fig_pct, grp_pct = time_series_pct_from_grp(COL_FORMAT, "Format % of Total Over Time", grp)

        with tab_dist:
            fig, tbl = donut_for(COL_FORMAT, "Format Distribution")
            if fig is None:
                st.info("No format data.")
            else:
                st.plotly_chart(fig, width="stretch")
                if st.checkbox("Show numbers table", value=False, key="tbl_format_pie"):
                    st.dataframe(tbl, width="stretch", hide_index=True, height=240)

        with tab_vol:
            if fig_line is None:
                st.info("Not enough data for format over time.")
            else:
                st.plotly_chart(fig_line, width="stretch")
                if st.checkbox("Show numbers table", value=False, key="tbl_format_line"):
                    pivot = (
                        grp.pivot(index="bucket", columns=COL_FORMAT, values="Articles")
                        .fillna(0).astype(int).reset_index()
                        .rename(columns={"bucket": "Time"})
                    )
                    st.dataframe(pivot, width="stretch", hide_index=True, height=240)

        with tab_pct:
            if fig_pct is None:
                st.info("Not enough data for format % over time.")
            else:
                st.plotly_chart(fig_pct, width="stretch")
                if st.checkbox("Show numbers table", value=False, key="tbl_format_pct"):
                    pivot = (
                        grp_pct.pivot(index="bucket", columns=COL_FORMAT, values="Percent")
                        .fillna(0).reset_index()
                        .rename(columns={"bucket": "Time"})
                    )
                    st.dataframe(pivot, width="stretch", hide_index=True, height=240)

    # GEOGRAPHIC SCOPE
    with p3:
        st.markdown("**Geographic Scope**")
        tab_dist, tab_vol, tab_pct = st.tabs(["Distribution", "Over time (volume)", "Over time (% of total)"])

        fig_line, grp = time_series_for(COL_SCOPE, "Geographic Scope Volume Over Time")
        fig_pct, grp_pct = time_series_pct_from_grp(COL_SCOPE, "Geographic Scope % of Total Over Time", grp)

        with tab_dist:
            fig, tbl = donut_for(COL_SCOPE, "Geographic Scope Distribution")
            if fig is None:
                st.info("No geographic scope data.")
            else:
                st.plotly_chart(fig, width="stretch")
                if st.checkbox("Show numbers table", value=False, key="tbl_scope_pie"):
                    st.dataframe(tbl, width="stretch", hide_index=True, height=240)

        with tab_vol:
            if fig_line is None:
                st.info("Not enough data for geographic scope over time.")
            else:
                st.plotly_chart(fig_line, width="stretch")
                if st.checkbox("Show numbers table", value=False, key="tbl_scope_line"):
                    pivot = (
                        grp.pivot(index="bucket", columns=COL_SCOPE, values="Articles")
                        .fillna(0).astype(int).reset_index()
                        .rename(columns={"bucket": "Time"})
                    )
                    st.dataframe(pivot, width="stretch", hide_index=True, height=240)

        with tab_pct:
            if fig_pct is None:
                st.info("Not enough data for geographic scope % over time.")
            else:
                st.plotly_chart(fig_pct, width="stretch")
                if st.checkbox("Show numbers table", value=False, key="tbl_scope_pct"):
                    pivot = (
                        grp_pct.pivot(index="bucket", columns=COL_SCOPE, values="Percent")
                        .fillna(0).reset_index()
                        .rename(columns={"bucket": "Time"})
                    )
                    st.dataframe(pivot, width="stretch", hide_index=True, height=240)
# ======================================================
# FEED (3 cards per row) — Search moved here (feed-only)
# ======================================================
feed_box = st.container(border=True)
with feed_box:
    st.markdown("### Filtered Articles Feed")

    ctl1, ctl2, ctl3 = st.columns([0.55, 0.25, 0.2], vertical_alignment="bottom")

    with ctl1:
        feed_search = st.text_input(
            "Search (Headline / Claim / Full Content)",
            value="",
            placeholder="type and press enter…",
            key="feed_search",
        )

    with ctl2:
        sort_order = st.selectbox("Sort feed by date", ["Most Recent", "Oldest"], index=0, key="feed_sort")

    with ctl3:
        view_mode = st.selectbox("View", ["Detailed", "Compact"], index=0, key="feed_view")

    df_feed = df_f.copy()

    # Apply search ONLY to feed
    if feed_search.strip():
        q = feed_search.strip().lower()
        df_feed = df_feed[
            df_feed[COL_HEADLINE].astype(str).str.lower().str.contains(q, na=False)
            | df_feed[COL_CLAIM].astype(str).str.lower().str.contains(q, na=False)
            | df_feed[COL_FULL].astype(str).str.lower().str.contains(q, na=False)
        ]

    df_feed = df_feed.sort_values(COL_DATE, ascending=(sort_order == "Oldest"), na_position="last")

    # Reset to page 1 when search changes
    if "feed_search_prev" not in st.session_state:
        st.session_state.feed_search_prev = ""
    if st.session_state.feed_search_prev != feed_search:
        st.session_state.page = 1
        st.session_state.feed_search_prev = feed_search

    PAGE_SIZE = 30
    if "page" not in st.session_state:
        st.session_state.page = 1

    total = len(df_feed)
    total_pages = max(1, math.ceil(total / PAGE_SIZE))
    st.session_state.page = min(max(1, st.session_state.page), total_pages)

    nav1, nav2, nav3 = st.columns([0.2, 0.6, 0.2], vertical_alignment="center")
    with nav1:
        if st.button("← Prev", disabled=(st.session_state.page <= 1), width="stretch", key="prev_btn"):
            st.session_state.page -= 1
            st.rerun()
    with nav2:
        showing = 0 if total == 0 else min(PAGE_SIZE, max(0, total - (st.session_state.page - 1) * PAGE_SIZE))
        st.caption(f"Page {st.session_state.page} / {total_pages}  •  Showing {showing} of {total} rows")
    with nav3:
        if st.button("Next →", disabled=(st.session_state.page >= total_pages), width="stretch", key="next_btn"):
            st.session_state.page += 1
            st.rerun()

    start_i = (st.session_state.page - 1) * PAGE_SIZE
    end_i = start_i + PAGE_SIZE
    page_df = df_feed.iloc[start_i:end_i].copy()
    rows = page_df.to_dict(orient="records")

    def make_unique_key(pos: int, link: str) -> str:
        base = f"{pos}|{link}"
        return hashlib.md5(base.encode("utf-8")).hexdigest()[:12]

    def render_card(row: dict, uniq: str):
        headline = as_str(row.get(COL_HEADLINE, "")).strip() or "(No headline)"
        date = row.get(COL_DATE, pd.NaT)
        date_str = date.strftime("%Y-%m-%d") if pd.notna(date) else ""
        site = as_str(row.get(COL_SITE, "")).strip()
        topic = as_str(row.get(COL_TOPIC, "")).strip()
        claim = as_str(row.get(COL_CLAIM, "")).strip()
        full = as_str(row.get(COL_FULL, "")).strip()
        link = as_str(row.get(COL_LINK, "")).strip()

        with st.container(border=True):
            st.markdown(f"**{headline}**")
            meta = " • ".join([x for x in [date_str, site, topic] if x])
            if meta:
                st.caption(meta)

            if view_mode == "Detailed":
                preview = claim if claim else (full[:280] + ("…" if len(full) > 280 else ""))
                if preview:
                    st.write(preview)
            else:
                if claim:
                    st.caption(claim[:140] + ("…" if len(claim) > 140 else ""))

            b1, b2 = st.columns([0.55, 0.45])
            with b1:
                open_full = st.button("View Full Article", key=f"open_{uniq}", width="stretch")
            with b2:
                if link:
                    st.link_button("Open Source Link", link, width="stretch")

            if open_full:
                if hasattr(st, "dialog"):
                    @st.dialog("Full Article")
                    def _show():
                        st.markdown(f"### {headline}")
                        if meta:
                            st.caption(meta)
                        if claim:
                            st.markdown("**Claim**")
                            st.write(claim)
                            st.markdown("---")
                        st.markdown("**Full Content**")
                        st.write(full if full else "(No full content)")
                    _show()
                else:
                    st.markdown("**Full Article**")
                    st.write(full if full else "(No full content)")

    # 3 cards per row
    pos = start_i
    for i in range(0, len(rows), 3):
        cA, cB, cC = st.columns(3, vertical_alignment="top")
        for col, row in zip([cA, cB, cC], rows[i:i + 3]):
            with col:
                link = as_str(row.get(COL_LINK, "")).strip()
                uniq = make_unique_key(pos, link)
                render_card(row, uniq)
                pos += 1

# ======================================================
# DEBUG
# ======================================================
with st.expander("Debug: show filtered table"):
    st.dataframe(df_f, width="stretch")