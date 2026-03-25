# Dashboard Architecture — Plain English Guide

## The Big Picture

The pipeline (`main.py`) does all the hard work: ingesting 10 million rows, running SQL transformations, and computing Six Sigma metrics. All of that work is stored permanently in the DuckDB database.

The dashboard (`dashboard.py`) does none of that work. It just *reads* from the database and draws pictures. Think of it as a window into the database — not a pipeline.

---

## How Streamlit Works

Streamlit is a Python library that turns a plain Python script into a web page. There is no HTML, no JavaScript, no web framework to learn. You write Python, and Streamlit figures out the browser part.

**The mental model:**

Every time a user interacts with the dashboard (clicks a button, moves a slider, types in a search box), Streamlit re-runs the entire Python script from top to bottom. That's it. That's the whole framework.

```
User clicks something
    → Streamlit re-runs dashboard.py top to bottom
    → Python fetches fresh data from DuckDB
    → Streamlit redraws the page with the new results
```

**What the Streamlit commands do in plain English:**

| Command | What it does |
|---|---|
| `st.title("text")` | Puts a big heading on the page |
| `st.subheader("text")` | Puts a smaller heading on the page |
| `st.metric("label", value)` | Draws a KPI card — a number with a label |
| `st.dataframe(df)` | Draws an interactive sortable table from a pandas DataFrame |
| `st.plotly_chart(fig)` | Draws a Plotly chart on the page |
| `st.tabs([...])` | Creates clickable tabs; content goes inside `with tab1:` blocks |
| `st.columns(2)` | Splits the page into side-by-side columns |
| `st.radio(...)` | Draws radio buttons the user can click to switch between options |
| `st.slider(...)` | Draws a slider the user can drag to change a number |
| `st.text_input(...)` | Draws a text box the user can type into |
| `st.sidebar` | Puts content in a collapsible panel on the left side of the page |
| `st.expander("label")` | Creates a collapsible section — hidden until the user clicks it |
| `st.divider()` | Draws a horizontal line |
| `st.caption("text")` | Draws small grey helper text |

**Caching — why it matters:**

If Streamlit re-runs the script on every click, and every run queries the database, that would be slow. Caching solves this:

```python
@st.cache_data
def query(sql):
    # This only runs ONCE per unique SQL string.
    # The result is stored in memory and reused on every re-run.
    return get_db().execute(sql).fetchdf()
```

`@st.cache_data` tells Streamlit: "run this function once, remember the result, and hand back the remembered result every time after that — unless the inputs change."

`@st.cache_resource` is similar but used for connections (like the DuckDB connection) that should be created once and shared across all users and all re-runs.

---

## How Plotly Works

Plotly is a charting library. It takes a pandas DataFrame and turns it into an interactive chart — one that users can hover over, zoom into, and pan around in the browser.

**The mental model:**

Plotly works in two steps:
1. **Build the figure** — describe what the chart should look like (what data, what type, what colors, what labels)
2. **Hand it to Streamlit** — `st.plotly_chart(fig)` renders it on the page

```
pandas DataFrame (rows and columns of data)
    → Plotly Express or Graph Objects (describe the chart)
    → fig object (a complete chart definition)
    → st.plotly_chart(fig) (drawn on the web page)
```

**Two ways to build a Plotly chart:**

**Plotly Express (`px`)** — the fast, simple way. One line of code for common chart types:

```python
import plotly.express as px

fig = px.bar(df, x="SIGMA_LEVEL", y="FIELD_FAMILY", orientation="h")
st.plotly_chart(fig)
```

Plain English: "Make a horizontal bar chart. Each bar is a field family. The length of the bar is its sigma level."

**Plotly Graph Objects (`go`)** — the detailed way. Used when you need more control, like combining two chart types on the same figure (the Pareto chart in Tab 3):

```python
import plotly.graph_objects as go

fig = go.Figure()
fig.add_trace(go.Bar(...))      # Add the bar chart layer
fig.add_trace(go.Scatter(...))  # Add the line chart layer on top
fig.update_layout(...)          # Control titles, axes, colors, size
st.plotly_chart(fig)
```

Plain English: "Start with a blank canvas. Add bars for financial variance. Add a line for cumulative percentage. Dress it up with labels and a second Y-axis."

---

## How DuckDB Fits In

DuckDB is the data layer. Streamlit and Plotly don't know or care where data comes from — they just need a pandas DataFrame. DuckDB's job is to produce that DataFrame from a SQL query against the pipeline database.

```
SQL query (a string of SELECT ... FROM view_name)
    → DuckDB executes it against data_eng.duckdb
    → Returns results as a pandas DataFrame
    → Plotly turns the DataFrame into a chart
    → Streamlit draws the chart on the page
```

The dashboard opens the DuckDB file in **read-only mode** — it can query but cannot change anything. The pipeline database is never at risk from the dashboard.

---

## How dashboard.py Is Organized

```
1. SETUP
   Load environment variables (.env → DUCKDB_PATH)
   Configure the page (title, icon, layout)

2. CONNECTIONS
   get_db()     → read-only connection to data_eng.duckdb (created once, cached)
   query(sql)   → runs a SQL string, returns a DataFrame (results cached per query)

3. PAGE HEADER
   Title and caption

4. LOAD SUMMARY DATA
   Query vw_sigma_analysis for the two top-level rows
   (Carrier Claims and Beneficiary Summary)
   Recalculate sigma levels using scipy (matches pipeline logic exactly)

5. TABS (four sections, each in a with tab: block)

   Tab 1 — Migration Scorecard
       Show KPI metric cards for each subject area
       Show sigma level reference table (built inline with a SQL VALUES clause)

   Tab 2 — Six Sigma by Field
       User picks subject area (radio button) and top-N (slider)
       Query the appropriate field-level view
       Draw a horizontal bar chart colored by sigma level (Plotly Express)
       Show full data table in a collapsible expander

   Tab 3 — Financial Impact
       User picks dataset (radio button)
       Query the financial impact view
       Draw a Pareto chart: bars for variance + line for cumulative % (Graph Objects)
       Show summary metrics below the chart

   Tab 4 — Discrepancy Drill-Down
       User picks report type (selectbox) and optional ID filter (text input)
       Build the SQL WHERE clause dynamically based on the filter
       Query the appropriate discrepancy view
       Show record count and scrollable data table
```

---

## Why This Architecture Makes Sense

| Concern | How It's Handled |
|---|---|
| Pipeline took 4-6 hours to run | Dashboard reads the already-populated database — zero re-run cost |
| Database has 10M+ rows | Views pre-aggregate the data; dashboard queries summaries, not raw tables |
| Slow queries would make the UI feel broken | `@st.cache_data` runs each query once and reuses the result |
| Database must not be modified | DuckDB connection opened with `read_only=True` |
| Two different chart types needed for the Pareto | Plotly Graph Objects lets you layer Bar + Scatter on the same figure |
| Sigma recalculation | scipy inverse normal applied in Python after the DB query — same formula the pipeline uses, so numbers are identical |
