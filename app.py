#hello Jay
import streamlit as st
import pandas as pd
import numpy as np
import json
import re
import time
from datetime import datetime, date
from snowflake.snowpark.context import get_active_session
import plotly.express as px
import plotly.graph_objects as go
# import base64   

# ========== SESSION & PAGE CONFIG ==========
session = get_active_session()
# b64_logo = "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"  # paste full string here
# image_string = f"data:image/png;base64,{b64_logo}"

# st.markdown(
#     f"""
#     <div style="display: flex; justify-content: flex-end; align-items: flex-start; width: 100%;">
#         <img src="{image_string}" width="200">
#     </div>
#     """,
#     unsafe_allow_html=True
# )
st.set_page_config(page_title="Data Quality Suite", layout="wide")
st.title("📊 Data Quality Validator")
st.caption("Run **Completeness & Validation** checks on your Snowflake table")

# ========== MODULE 1: STATE + HELPERS (AI detection + constraint editor) ==========
def _ensure_state():
    st.session_state.setdefault("dq_ready", False)
    st.session_state.setdefault("df", None)
    st.session_state.setdefault("col_descriptions", {})
    st.session_state.setdefault("constraints", {})  # dict of col -> list of {"type","value","source"}
    st.session_state.setdefault("active_col", None)
    st.session_state.setdefault("edit_mode", None)
    st.session_state.setdefault("result_df", None)
    st.session_state.setdefault("refresh_flag", False)
    st.session_state.setdefault("ai_suggestions", {})  # store ai suggestions like regex and min/max
_ensure_state()

def normalize_type_from_ai(s: str):
    s = s.strip().upper()
    if s in ["NOT NULL", "NOT_NULL", "NOTNULL"]:
        return "Not Null"
    if s in ["REGEX", "REGEX MATCH", "REGEX_MATCH"]:
        return "Regex Match"
    if s in ["MIN", "MIN VALUE"]:
        return "Min Value"
    if s in ["MAX", "MAX VALUE"]:
        return "Max Value"
    if s in ["VALIDATION"]:
        return "Validation"
    if s in ["TIMELINESS"]:
        return "Timeliness"
    if "VALUES ALLOWED" in s:
        return "Values Allowed (comma-separated)"
    if "VALUES NOT" in s:
        return "Values Not Allowed (comma-separated)"
    return s.title()

def normalize_type_for_compare(display_type: str):
    return re.sub(r'[^a-z0-9]', '', display_type).lower()

# helper to render selectbox with visually disabled options
def render_selectbox_with_disabled(label, options, disabled_opts, default, key):
    labels = []
    for opt in options:
        if opt in disabled_opts:
            labels.append(f"{opt} (disabled)")
        else:
            labels.append(opt)
    if default in disabled_opts:
        default_label = f"{default} (disabled)"
    else:
        default_label = default
    if default_label not in labels:
        default_idx = 0
    else:
        default_idx = labels.index(default_label)
    selected_label = st.selectbox(label, labels, index=default_idx, key=key)
    disabled_selected = selected_label.endswith(" (disabled)")
    selected = selected_label.replace(" (disabled)", "")
    return selected, disabled_selected

# ================== VALIDATION HELPERS (kept & reused) ==================
DATE_REGEX_STRICT = r"^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$"

def _is_leap(y: int) -> bool:
    return (y % 4 == 0 and y % 100 != 0) or (y % 400 == 0)

def _validate_yyyy_mm_dd(value):
    if pd.isnull(value) or str(value).strip() == "":
        return False, "Date is missing"
    s = str(value).strip()
    if not re.fullmatch(DATE_REGEX_STRICT, s):
        return False, "Invalid date format (expected YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)"
    try:
        if " " in s:  # has time part
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        else:
            dt = datetime.strptime(s, "%Y-%m-%d")
    except Exception:
        return False, "Invalid date parsing"
    y, m, d = dt.year, dt.month, dt.day
    if not (1 <= m <= 12):
        return False, "Invalid month"
    if m in {1, 3, 5, 7, 8, 10, 12} and not (1 <= d <= 31):
        return False, "Invalid day"
    if m in {4, 6, 9, 11} and not (1 <= d <= 30):
        return False, "Invalid day"
    if m == 2:
        if _is_leap(y) and not (1 <= d <= 29):
            return False, "Invalid Feb day leap"
        if not _is_leap(y) and not (1 <= d <= 28):
            return False, "Invalid Feb day non-leap"
    return True, None

def _parse_strict_date(value):
    ok, _ = _validate_yyyy_mm_dd(value)
    if not ok:
        return None
    s = str(value).strip()
    try:
        if " " in s:
            return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        else:
            return datetime.strptime(s, "%Y-%m-%d")
    except Exception:
        return None

# ================== COLUMN VALIDATORS ==================
def validate_name(df, col: str):
    results = []
    for idx, val in df[col].items():
        s = str(val).strip()
        if not s:
            results.append((col, idx, val, False, "Name missing"))
        elif len(s) > 40:
            results.append((col, idx, val, False, "Too long"))
        else:
            results.append((col, idx, val, True, None))
    return pd.DataFrame(results, columns=["Column","RowIndex","Value","IsValid","ErrorMessage"])

def validate_gender(df, col: str) :
    results = []
    for idx, val in df[col].items():
        sx = str(val).strip().upper()
        if not sx:
            results.append((col, idx, val, False, "Missing"))
        elif sx not in {"M","F","OTHER","MALE","FEMALE"}:
            results.append((col, idx, val, False, "Invalid"))
        else:
            results.append((col, idx, val, True, None))
    return pd.DataFrame(results, columns=["Column","RowIndex","Value","IsValid","ErrorMessage"])

def validate_phone(df, col: str) :
    results = []
    for idx, val in df[col].items():
        digits = re.sub(r"\D", "", str(val))
        if len(digits) != 10:
            results.append((col, idx, val, False, "Invalid phone"))
        else:
            results.append((col, idx, val, True, None))
    return pd.DataFrame(results, columns=["Column","RowIndex","Value","IsValid","ErrorMessage"])

def validate_email(df, col: str) :
    results = []
    for idx, val in df[col].items():
        s = str(val).strip()
        if not s:
            results.append((col, idx, val, False, "Missing"))
        elif not re.fullmatch(r"^[\w\.-]+@[\w\.-]+\.\w+$", s):
            results.append((col, idx, val, False, "Invalid email"))
        else:
            results.append((col, idx, val, True, None))
    return pd.DataFrame(results, columns=["Column","RowIndex","Value","IsValid","ErrorMessage"])

def validate_postal(df, col: str) :
    results = []
    for idx, val in df[col].items():
        if not re.fullmatch(r"\d{6}", str(val).strip()):
            results.append((col, idx, val, False, "Invalid postal"))
        else:
            results.append((col, idx, val, True, None))
    return pd.DataFrame(results, columns=["Column","RowIndex","Value","IsValid","ErrorMessage"])

def validate_amount(df, col: str) :
    results = []
    for idx, val in df[col].items():
        s = str(val).strip()
        if not s:
            results.append((col, idx, val, False, "Missing"))
            continue
        try:
            num = float(re.sub(r"[^\d\.\-]", "", s).replace(",", ""))
            if num <= 0:
                results.append((col, idx, val, False, "Not positive"))
            else:
                results.append((col, idx, val, True, None))
        except:
            results.append((col, idx, val, False, "Invalid amount"))
    return pd.DataFrame(results, columns=["Column","RowIndex","Value","IsValid","ErrorMessage"])

def validate_age(df, col: str):
    results = []
    for idx, val in df[col].items():
        try:
            age = int(val)
            if age <= 0 or age > 120:
                results.append((col, idx, val, False, "Invalid age"))
            else:
                results.append((col, idx, val, True, None))
        except:
            results.append((col, idx, val, False, "Invalid age"))
    return pd.DataFrame(results, columns=["Column","RowIndex","Value","IsValid","ErrorMessage"])

# helper: when a constraint is "Validation" we map to a built-in validator dynamically based on column name
def choose_builtin_validator_by_column(col_name):
    col_lower = col_name.lower()
    if "email" in col_lower:
        return validate_email
    if "phone" in col_lower or "contact" in col_lower or "mobile" in col_lower:
        return validate_phone
    if "gender" in col_lower:
        return validate_gender
    if "name" in col_lower or "first" in col_lower or "last" in col_lower:
        return validate_name
    if "age" in col_lower:
        return validate_age
    if "postal" in col_lower or "pin" in col_lower or "zip" in col_lower:
        return validate_postal
    if "amount" in col_lower or "price" in col_lower or "cost" in col_lower:
        return validate_amount
    return None

# ================== TIMELINESS DETECTION & VALIDATION ==================
def detect_date_columns(df) -> list[str]:
    date_candidates = []
    simple_pattern = DATE_REGEX_STRICT
    for col in df.columns:
        col_lower = col.lower()
        if "date" in col_lower:
            date_candidates.append(col)
            continue
        sample = df[col].dropna().astype(str).head(30)
        if sample.empty:
            continue
        frac_match = sample.str.match(simple_pattern).mean()
        if frac_match >= 0.40:
            date_candidates.append(col)
    return date_candidates

def validate_timeliness(df, date_cols: list[str], mask: pd.DataFrame, summary_rows: list):
    dob_col = next((c for c in date_cols if "birth" in c.lower()), None)
    admission_col = next((c for c in date_cols if "admission" in c.lower()), None)
    discharge_col = next((c for c in date_cols if "discharge" in c.lower()), None)
    parsed = {}
    for col in date_cols:
        parsed[col] = df[col].apply(_parse_strict_date)
    if dob_col:
        for col in date_cols:
            if col == dob_col:
                continue
            invalid_idx = (
                parsed[col].notna()
                & parsed[dob_col].notna()
                & (parsed[col] <= parsed[dob_col])
            )
            mask.loc[invalid_idx, col] = True
            if invalid_idx.any():
                summary_rows.append({
                    "Column": col,
                    "Invalid Count": int(invalid_idx.sum()),
                    "Description": f"{col} must be after {dob_col}"
                })
    if admission_col and discharge_col:
        invalid_idx = (
            parsed[admission_col].notna()
            & parsed[discharge_col].notna()
            & (parsed[discharge_col] < parsed[admission_col])
        )
        mask.loc[invalid_idx, discharge_col] = True
        if invalid_idx.any():
            summary_rows.append({
                "Column": f"{admission_col} vs {discharge_col}",
                "Invalid Count": int(invalid_idx.sum()),
                "Description": f"{discharge_col} must be on/after {admission_col}"
            })

# =================== DYNAMIC run_validation (reads constraints_df) ===================
def run_validation(df: pd.DataFrame, constraints_df: pd.DataFrame):
    """
    constraints_df expected columns:
      - column_name
      - constraint_type
      - value
      - source (optional)
    This function:
      - applies constraints dynamically
      - applies builtin validators when a "Validation" constraint is present (using column name heuristics)
      - applies timeliness check for all columns marked Timeliness OR inferred date columns
    """
    mask = pd.DataFrame(False, index=df.index, columns=df.columns)
    summary_rows = []
    all_results = []

    # Build list of timeliness columns requested
    timeliness_cols = []

    if constraints_df is None or constraints_df.empty:
        constraints_df = pd.DataFrame(columns=["column_name", "constraint_type", "value", "source"])

    for _, row in constraints_df.iterrows():
        col = row.get("column_name")
        if col not in df.columns:
            # skip unknown column (safe guard)
            continue
        ctype = str(row.get("constraint_type", "")).strip()
        cvalue = row.get("value", "")

        lower_ct = ctype.strip().lower()

        # TIMELINESS deferred: collect columns where ctype is Timeliness
        if lower_ct == "timeliness":
            timeliness_cols.append(col)
            continue

        # Initialize failed rows sentinel
        failed_rows = pd.Series(False, index=df.index)

        # NOT NULL
        if lower_ct == "not null":
            failed_rows = df[col].isna() | (df[col].astype(str).str.strip() == "")

        # REGEX MATCH
        elif lower_ct == "regex match":
            try:
                pattern = re.compile(str(cvalue))
                failed_rows = ~df[col].astype(str).str.match(pattern, na=False)
            except re.error:
                # invalid regex -> mark as error in summary but do not mask rows
                summary_rows.append({
                    "Column": col,
                    "Invalid Count": 0,
                    "Description": f"Invalid regex pattern: {cvalue}"
                })
                continue

        # VALUES ALLOWED
        elif "values allowed" in lower_ct:
            allowed = [v.strip() for v in str(cvalue).split(",") if v.strip() != ""]
            if pd.api.types.is_integer_dtype(df[col]):
                try:
                    allowed_parsed = [int(x) for x in allowed]
                    failed_rows = ~df[col].isin(allowed_parsed)
                except:
                    failed_rows = pd.Series(False, index=df.index)
            else:
                failed_rows = ~df[col].astype(str).isin(allowed)

        # VALUES NOT ALLOWED
        elif "values not allowed" in lower_ct:
            not_allowed = [v.strip() for v in str(cvalue).split(",") if v.strip() != ""]
            if pd.api.types.is_integer_dtype(df[col]):
                try:
                    not_allowed_parsed = [int(x) for x in not_allowed]
                    failed_rows = df[col].isin(not_allowed_parsed)
                except:
                    failed_rows = pd.Series(False, index=df.index)
            else:
                failed_rows = df[col].astype(str).isin(not_allowed)

        # MIN VALUE
        elif lower_ct == "min value":
            try:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    threshold = pd.to_datetime(cvalue)
                    failed_rows = df[col] < threshold
                else:
                    threshold = float(cvalue)
                    failed_rows = df[col].astype(float) < threshold
            except Exception:
                summary_rows.append({
                    "Column": col,
                    "Invalid Count": 0,
                    "Description": f"Invalid Min Value: {cvalue}"
                })
                continue

        # MAX VALUE
        elif lower_ct == "max value":
            try:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    threshold = pd.to_datetime(cvalue)
                    failed_rows = df[col] > threshold
                else:
                    threshold = float(cvalue)
                    failed_rows = df[col].astype(float) > threshold
            except Exception:
                summary_rows.append({
                    "Column": col,
                    "Invalid Count": 0,
                    "Description": f"Invalid Max Value: {cvalue}"
                })
                continue

        # VALIDATION -> use builtin validator chosen dynamically by column name
        elif lower_ct == "validation":
            validator = choose_builtin_validator_by_column(col)
            if validator is not None:
                col_results = validator(df, col)
                all_results.append(col_results)
                invalid_rows = col_results.loc[~col_results["IsValid"], "RowIndex"].tolist()
                if invalid_rows:
                    mask.loc[invalid_rows, col] = True
                    grouped = col_results.loc[~col_results["IsValid"]].groupby("ErrorMessage").size()
                    for err, count in grouped.items():
                        summary_rows.append({
                            "Column": col,
                            "Invalid Count": int(count),
                            "Description": err
                        })
            else:
                # no builtin validator found — skip
                summary_rows.append({
                    "Column": col,
                    "Invalid Count": 0,
                    "Description": f"No builtin validator mapped for column '{col}'"
                })
            continue

        # If we got here, we have a failed_rows boolean Series to apply
        if isinstance(failed_rows, pd.Series):
            mask.loc[failed_rows, col] = True
            if failed_rows.any():
                summary_rows.append({
                    "Column": col,
                    "Invalid Count": int(failed_rows.sum()),
                    "Description": f"{ctype} violation"
                })

    # After all constraints: handle TIMELINESS constraints
    # If user explicitly provided timeliness columns (timeliness_cols), use those.
    # Otherwise infer date columns and run default timeliness checks.
    if timeliness_cols:
        date_cols_for_timeliness = timeliness_cols
    else:
        date_cols_for_timeliness = detect_date_columns(df)

    if date_cols_for_timeliness:
        validate_timeliness(df, date_cols_for_timeliness, mask, summary_rows)

    # Combine results
    all_results_df = pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame(
        columns=["Column","RowIndex","Value","IsValid","ErrorMessage"]
    )
    summary_df = pd.DataFrame(summary_rows) if summary_rows else pd.DataFrame(
        columns=["Column","Invalid Count","Description"]
    )

    return summary_df, mask, all_results_df

# helper for missing mask
def build_missing_mask(df):
    empty_str = df.astype(str).apply(
        lambda c: (c.str.strip() == "") | 
                  (c.str.strip() == "-") |
                  (c.str.strip().str.lower() == "none") | 
                  (c.str.strip().str.lower() == "null") |
                  (c.str.strip().str.lower() == "na")
    )
    return df.isna() | empty_str

# ========== UI: Database / Schema / Table (single unified selector) ==========
dbs = [r[1] for r in session.sql("SHOW DATABASES").collect()]
database = st.selectbox("Select Database", dbs, key="db_sel")

schemas = [r[1] for r in session.sql(f"SHOW SCHEMAS IN DATABASE {database}").collect()] if database else []
schema = st.selectbox("Select Schema", schemas, key="schema_sel")

tables = [r[1] for r in session.sql(f"SHOW TABLES IN SCHEMA {database}.{schema}").collect()] if schema else []
table = st.selectbox("Select Table", tables, key="table_sel")




# 🔹 Column list for optional selection
available_columns = []
if database and schema and table:
    try:
        col_info = session.sql(f"SHOW COLUMNS IN TABLE {database}.{schema}.{table}").collect()
        # Different Snowflake versions / contexts may return different casing; try both
        if len(col_info) > 0:
            # collect column name safely
            for r in col_info:
                # prefer dictionary-style access if available
                try:
                    col_name = r["column_name"]
                except Exception:
                    # fallback by index
                    col_name = list(r.values())[2] if len(list(r.values())) > 2 else None
                if col_name:
                    available_columns.append(col_name)
    except Exception:
        available_columns = []

selected_columns = st.multiselect(
    "Select Specific Columns (optional — leave empty to include all)",
    options=available_columns,
    default=[]
)

# 🔹 Customizable sample percentage for AI inference
sample_percent = st.slider(
    "Select Sample Percentage for AI Inference (of total rows)",
    min_value=1,
    max_value=100,
    value=10,
    step=1,
    help="Percentage of table rows to use for stratified sampling during AI inference"
)




# ========== MODULE 1: "Go" — load table, run AI inference (keeps original behavior) ==========
if st.button("🚀 Go") and database and schema and table:
    start_time = time.time()
    with st.spinner("Loading table and Auto Detecting Datatypes And Constraints..."):
        fqtn = f"{database}.{schema}.{table}"

        # 🔹 Load only selected columns if specified
        try:
            if selected_columns and len(selected_columns) > 0:
                cols_str = ", ".join([f'"{c}"' for c in selected_columns])
                df = session.sql(f"SELECT {cols_str} FROM {fqtn}").to_pandas()
            else:
                df = session.table(fqtn).to_pandas()
        except Exception as e:
            st.error(f"Failed to load table: {e}")
            st.stop()

        st.session_state["df"] = df

        # Column descriptions
        try:
            desc_query = f"""
                SELECT column_name, comment
                FROM {database}.information_schema.columns
                WHERE table_schema='{schema}' AND table_name='{table}'
            """
            col_meta = session.sql(desc_query).collect()
            col_descriptions = {row["COLUMN_NAME"]: (row.get("COMMENT") or "") for row in col_meta}
        except Exception:
            col_descriptions = {}

        # 🔹 Use user-selected sample percentage
        sample_frac = max(0.01, min(1.0, float(sample_percent) / 100.0))
        seed = 123
        sample_size = max(1, int(len(df) * sample_frac))

        samples = {}
        for col in df.columns:
            try:
                counts = df[col].value_counts(dropna=True)
                total = counts.sum() if counts.size > 0 else 1
                ideal = counts * sample_size / total if total > 0 else pd.Series(dtype=int)
                floored = ideal.astype(int)
                remainder = ideal - floored
                allocation = floored.copy()
                shortfall = sample_size - allocation.sum()
                if shortfall > 0 and not remainder.empty:
                    top_remainders = remainder.nlargest(shortfall).index
                    allocation[top_remainders] += 1
                sample_vals = []
                for v, k in allocation.items():
                    sample_vals.extend([v] * k)
                if len(sample_vals) == 0:
                    sample_vals = df[col].dropna().sample(min(sample_size, max(1, len(df[col].dropna()))), random_state=seed).tolist() if len(df[col].dropna()) > 0 else []
                sample_series = pd.Series(sample_vals).sample(frac=1, random_state=seed).tolist()
                samples[col] = sample_series
            except Exception:
                samples[col] = df[col].dropna().sample(min(sample_size, max(1, len(df[col].dropna()))), random_state=seed).tolist() if len(df[col].dropna()) > 0 else []

        sample_df = pd.DataFrame(samples)
        sample_text = sample_df.to_json(orient="records")

        prompt = f"""
You are a data quality and schema inference assistant.
Here is a stratified sample ({sample_size} rows, {sample_percent}% of actual data) from the table in JSON:
{sample_text}

For each column:
- Detect datatype (int, float, str, bool, date, datetime).
- Be noise-tolerant: ignore a small percentage of irregular, null, or inconsistent values when determining datatype.
  For example, if 95% of values look numeric and 5% are invalid strings, treat the column as numeric.
- Detect constraints (choose only from: NOT NULL, VALIDATION, TIMELINESS).
- Provide a short plain-English description of what the column represents.
- Generate a practical and generalized regex pattern that can validate the majority of real values **without overfitting**.

When generating regex patterns, apply this decision logic carefully:

1. **Categorical Columns (few unique values):**
   - If the column has a small number of distinct values (for example ≤10 unique values or <5% unique ratio),
     treat it as categorical.
   - Regex should only allow those valid values (case-insensitive).
     Examples:
       - Gender → ^(?i)(male|female|others)$
       - Department → ^(?i)(sales|hr|finance|it|admin)$
       - Status → ^(?i)(active|inactive|terminated)$
   - Avoid simple alphabet-only regex; focus on allowed category values.

2. **Structured or Pattern-Based Columns:**
   - If most values are numeric → generate a numeric regex (e.g., ^\\d+$, or for phone numbers ^\\+?\\d{{10,15}}$).
   - If most values are alphabetic (like names, cities) → use ^[A-Za-z ]+$ (letters and spaces only).
   - If values resemble dates/times → use ^\\d{{4}}-\\d{{2}}-\\d{{2}}$ or similar.
   - If values mix letters and numbers (like IDs or codes) → use ^[A-Za-z0-9-]+$.
   - Do not hardcode specific sample values; generalize based on observed format patterns.

3. **Noise Handling:**
   - If a small number of outliers don’t fit the main pattern, ignore them.
   - Base your datatype and regex decision on the **majority pattern** in the data.

4. The regex should be **case-insensitive** where applicable and **cover the general valid structure**, not specific examples.

Return ONLY a valid JSON array.
Each object must use these exact keys:
- column_name
- detected_datatype
- description
- applied_constraints
- regex_pattern
"""

        # Call Snowflake AI_COMPLETE (keeps your original call)
        query = f"SELECT AI_COMPLETE('llama3.3-70b', $$ {prompt} $$) AS RAW_JSON"
        rows = session.sql(query).collect()
        raw_text = rows[0]["RAW_JSON"]
        match = re.search(r"\[.*\]", raw_text, re.S)
        if match:
            json_text = match.group(0).strip()
            json_text = json_text.replace("\\n", "\n").replace("\\t", "\t").replace('\\"', '"').replace("\\\\", "\\")
            json_text = re.sub(r",\s*]", "]", json_text)
            json_text = re.sub(r",\s*}", "}", json_text)
            try:
                raw_results = json.loads(json_text)
            except json.JSONDecodeError:
                raw_results = []
            result_df = pd.json_normalize(raw_results)
            st.session_state["result_df"] = result_df

            # Pre-load constraints from result_df
            for col in df.columns:
                if col in result_df["column_name"].values:
                    pre_cons = result_df.loc[result_df["column_name"]==col,"applied_constraints"].values[0]
                    ai_regex = result_df.loc[result_df["column_name"]==col,"regex_pattern"].values[0] if "regex_pattern" in result_df.columns else ""
                    col_min = None
                    col_max = None
                    try:
                        if pd.api.types.is_integer_dtype(df[col]) or pd.api.types.is_float_dtype(df[col]):
                            col_min = df[col].min(skipna=True)
                            col_max = df[col].max(skipna=True)
                        if pd.api.types.is_datetime64_any_dtype(df[col]):
                            col_min = df[col].min(skipna=True)
                            col_max = df[col].max(skipna=True)
                    except Exception:
                        pass
                    st.session_state["ai_suggestions"].setdefault(col, {})
                    st.session_state["ai_suggestions"][col]["regex"] = ai_regex or ""
                    st.session_state["ai_suggestions"][col]["min"] = col_min
                    st.session_state["ai_suggestions"][col]["max"] = col_max

                    parsed = []
                    if isinstance(pre_cons, (list, tuple)):
                        for pc in pre_cons:
                            try:
                                parsed.append({"type": normalize_type_from_ai(str(pc)), "value": "", "source":"ai"})
                            except:
                                parsed.append({"type": str(pc), "value":"", "source":"ai"})
                    else:
                        try:
                            for pc in str(pre_cons).split(","):
                                parsed.append({"type": normalize_type_from_ai(pc), "value":"", "source":"ai"})
                        except:
                            parsed.append({"type": normalize_type_from_ai(str(pre_cons)), "value":"", "source":"ai"})
                    st.session_state["constraints"][col] = parsed
                else:
                    st.session_state["constraints"][col] = []

        st.session_state["col_descriptions"] = col_descriptions
    elapsed = round(time.time() - start_time, 2)
    st.success(f"Detected And Applied Constraints in {elapsed} seconds")
    st.session_state["dq_ready"] = True

# ========== RENDER MODULE 1 UI (columns list + manage buttons) ==========
if st.session_state["dq_ready"] and st.session_state["df"] is not None:
    df = st.session_state["df"]
    result_df = st.session_state.get("result_df", None)
    col_descriptions = st.session_state["col_descriptions"]

    st.subheader("📝 Column Metadata & Constraints (Tabular View)")
    header_cols = st.columns([2, 3, 3, 2, 2])
    with header_cols[0]: st.markdown("**Column**")
    with header_cols[1]: st.markdown("**Description**")
    with header_cols[2]: st.markdown("**Auto Detected Datatype**")
    with header_cols[3]: st.markdown("**Applied Constraints**")
    with header_cols[4]: st.markdown("**Manage**")

    python_dtypes = ["int", "float", "str", "bool", "date", "datetime"]
    dtype_map = {"int64":"int", "Int64":"int","float64":"float","object":"str","bool":"bool",
                 "datetime64[ns]":"datetime","datetime64[ns, UTC]":"datetime","datetime64":"datetime","date":"date"}

    for col in df.columns:
        row_cols = st.columns([2,3,3,2,2])
        with row_cols[0]: st.write(col)
        with row_cols[1]:
            desc_val = st.session_state["col_descriptions"].get(col, "—")
            if result_df is not None and col in result_df["column_name"].values:
                desc_val = result_df.loc[result_df["column_name"]==col, "description"].values[0]
            st.write(desc_val)
        with row_cols[2]:
            dtype_val = dtype_map.get(str(df[col].dtype),"str")
            if result_df is not None and col in result_df["column_name"].values:
                dtype_val = result_df.loc[result_df["column_name"]==col,"detected_datatype"].values[0]
                if dtype_val not in python_dtypes: dtype_val="str"
            selected_type = st.selectbox("", python_dtypes, index=python_dtypes.index(dtype_val), key=f"dtype_{col}", label_visibility="collapsed")
            try:
                if selected_type=="int": df[col]=pd.to_numeric(df[col],errors="coerce").astype("Int64")
                elif selected_type=="float": df[col]=pd.to_numeric(df[col],errors="coerce")
                elif selected_type=="str": df[col]=df[col].astype(str)
                elif selected_type=="bool": df[col]=df[col].astype(bool)
                elif selected_type in ["datetime","date"]: df[col]=pd.to_datetime(df[col],errors="coerce")
            except: pass
            st.session_state["df"]=df
        with row_cols[3]:
            constraints = st.session_state["constraints"].get(col, [])
            if constraints:
                for c in constraints:
                    src_note = " (AI)" if c.get("source","")=="ai" else ""
                    st.write(f"- {c['type']}{src_note}" + (f" = {c['value']}" if c["value"] else ""))
            else:
                st.caption("—")
        with row_cols[4]:
            if st.button("⚙️ Manage", key=f"manage_{col}"):
                st.session_state["active_col"]=col

    # Sidebar constraint editor
    if st.session_state["active_col"]:
        active_col = st.session_state["active_col"]
        df = st.session_state["df"]
        existing = st.session_state["constraints"].get(active_col, [])
        ai_suggestions = st.session_state.get("ai_suggestions", {}).get(active_col, {})

        with st.sidebar:
            st.header(f"⚙️ Manage Constraints for `{active_col}`")
            if existing:
                for i,c in enumerate(existing):
                    cols = st.columns([6,1,1])
                    with cols[0]:
                        src_note = " (AI)" if c.get("source","")=="ai" else ""
                        st.write(f"- {c['type']}{src_note}" + (f" = {c['value']}" if c["value"] else ""))
                    with cols[1]:
                        if st.button("✏️",key=f"edit_{active_col}_{i}"):
                            st.session_state["edit_mode"]={"col":active_col,"idx":i,"constraint":c}
                    with cols[2]:
                        if st.button("❌",key=f"del_{active_col}_{i}"):
                            st.session_state["constraints"][active_col].pop(i)
                            st.session_state["edit_mode"]=None
                            st.rerun()

            edit_mode = st.session_state.get("edit_mode",None)
            default_type = edit_mode["constraint"]["type"] if edit_mode else "Not Null"
            default_value = edit_mode["constraint"]["value"] if edit_mode else ""

            constraint_types=[
                "Not Null","Regex Match","Min Value","Max Value",
                "Validation","Timeliness",
                "Values Allowed (comma-separated)","Values Not Allowed (comma-separated)"
            ]
            col_is_datetime = pd.api.types.is_datetime64_any_dtype(df[active_col])
            col_is_int = pd.api.types.is_integer_dtype(df[active_col]) or str(df[active_col].dtype).startswith("Int64")
            col_is_float = pd.api.types.is_float_dtype(df[active_col])
            col_is_str = pd.api.types.is_object_dtype(df[active_col]) or df[active_col].dtype == "string"

            disabled_options = []
            if col_is_datetime:
                ctype_options = [t for t in constraint_types if "Values" not in t]
            elif col_is_str:
                ctype_options = constraint_types[:]  # keep all visible
                disabled_options = ["Min Value", "Max Value"]
            else:
                ctype_options = [t for t in constraint_types if t!="Timeliness"]

            if default_type not in ctype_options:
                default_type=ctype_options[0]

            ctype, disabled_selected = render_selectbox_with_disabled(
                "Constraint Type", ctype_options, disabled_options, default_type, f"ctype_sidebar_{active_col}"
            )

            cvalue=""
            valid_input=True
            validation_message = ""

            if edit_mode:
                default_type = edit_mode["constraint"]["type"]
                default_value = edit_mode["constraint"]["value"]

            ai_regex_default = ai_suggestions.get("regex","") if ai_suggestions else ""
            ai_min = ai_suggestions.get("min", None)
            ai_max = ai_suggestions.get("max", None)

            if ctype in ["Not Null","Validation"]:
                cvalue=""
            elif ctype in ["Values Allowed (comma-separated)","Values Not Allowed (comma-separated)"]:
                inp = st.text_input("Constraint Value (comma-separated)",value=default_value,key=f"cvalue_sidebar_{active_col}")
                cvalue = inp
                if col_is_int:
                    vals = [v.strip() for v in inp.split(",") if v.strip()!=""]
                    for v in vals:
                        try:
                            int(v)
                        except:
                            valid_input=False
                            validation_message = "For integer columns, Allowed/Not Allowed values must be integers (no strings)."
                            break
            elif ctype=="Timeliness":
                num_datetime_cols = sum(1 for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c]))
                if num_datetime_cols < 1:
                    st.warning("⚠️ No datetime columns present in the table.")
                    valid_input=False
                    cvalue=""
                else:
                    options = [i for i in range(1, num_datetime_cols+1)]
                    try:
                        default_order = int(default_value) if default_value not in [None, ""] else None
                    except:
                        default_order = None
                    if default_order not in options:
                        default_order = options[-1]
                    cvalue = st.selectbox(f"Timeliness Order (1..{num_datetime_cols})", options, index=options.index(default_order), key=f"order_{active_col}")
            elif ctype in ["Min Value","Max Value"]:
                if col_is_int or col_is_float:
                    suggested = ai_min if ctype=="Min Value" else ai_max
                    if suggested is None or (isinstance(suggested, float) and pd.isna(suggested)):
                        suggested = df[active_col].min(skipna=True) if ctype=="Min Value" else df[active_col].max(skipna=True)
                    if suggested is None or (isinstance(suggested, float) and pd.isna(suggested)):
                        suggested = 0
                    if col_is_int:
                        try:
                            suggested_int = int(suggested)
                        except Exception:
                            try:
                                suggested_int = int(float(suggested))
                            except Exception:
                                suggested_int = 0
                        cvalue = st.number_input("Constraint Value", value=int(default_value) if default_value else suggested_int, step=1, format="%d", key=f"cvalue_sidebar_{active_col}")
                    else:
                        try:
                            suggested_float = float(suggested)
                        except Exception:
                            suggested_float = 0.0
                        cvalue = st.number_input("Constraint Value", value=float(default_value) if default_value else suggested_float, key=f"cvalue_sidebar_{active_col}")
                elif col_is_datetime:
                    suggested = ai_min if ctype=="Min Value" else ai_max
                    if suggested is None or (isinstance(suggested, float) and pd.isna(suggested)):
                        suggested = df[active_col].min(skipna=True) if ctype=="Min Value" else df[active_col].max(skipna=True)
                    if isinstance(suggested, pd.Timestamp):
                        suggested_date = suggested.to_pydatetime().date()
                    elif isinstance(suggested, datetime):
                        suggested_date = suggested.date()
                    elif isinstance(suggested, date):
                        suggested_date = suggested
                    else:
                        suggested_date = date.today()
                    cvalue = st.date_input("Constraint Value (date)", value=pd.to_datetime(default_value).date() if default_value else suggested_date, key=f"cvalue_sidebar_{active_col}")
                else:
                    cvalue = st.text_input("Constraint Value",value=default_value,key=f"cvalue_sidebar_{active_col}")
            elif ctype=="Regex Match":
                default_regex = default_value if default_value else (ai_regex_default or "")
                cvalue = st.text_input("Regex Pattern", value=default_regex, key=f"cvalue_sidebar_{active_col}")
                try:
                    re.compile(cvalue)
                except:
                    valid_input=False
                    validation_message = "Invalid regular expression pattern."
            else:
                cvalue = st.text_input("Constraint Value",value=default_value,key=f"cvalue_sidebar_{active_col}")

            chosen_key = normalize_type_for_compare(ctype)
            ai_has_same = any((normalize_type_for_compare(c["type"])==chosen_key and c.get("source","")=="ai") for c in existing)
            manual_has_same = any((normalize_type_for_compare(c["type"])==chosen_key and c.get("source","")!="ai") for c in existing)

            if ai_has_same and not edit_mode:
                st.info(f"⚠️ This constraint type `{ctype}` was already applied automatically by AI and cannot be re-applied.") 
                can_save = False
            else:
                can_save = True

            if not valid_input:
                st.error(validation_message)

            col1,col2=st.columns(2)
            with col1:
                if st.button("✅ Save Constraint",key=f"apply_sidebar_{active_col}") and valid_input and can_save:
                    if ctype in disabled_options:
                        st.warning(f"⚠️ `{ctype}` is disabled for string columns and cannot be saved.")
                    else:
                        source = "manual"
                        if edit_mode:
                            source = edit_mode["constraint"].get("source", "manual")
                        new={"type":ctype,"value":str(cvalue),"source":source}
                        if edit_mode:
                            st.session_state["constraints"][active_col][edit_mode["idx"]] = new
                            st.session_state["edit_mode"]=None
                        else:
                            if any(normalize_type_for_compare(c["type"])==normalize_type_for_compare(ctype) for c in st.session_state["constraints"].get(active_col,[])):
                                st.warning("A similar constraint already exists on this column.")
                            else:
                                st.session_state["constraints"].setdefault(active_col,[]).append(new)
                        st.rerun()
            with col2:
                if st.button("🧹 Clear All",key=f"clear_sidebar_{active_col}"):
                    st.session_state["constraints"][active_col] = []
                    st.session_state["edit_mode"]=None
                    st.rerun()

    # --- Run Data Quality Check Button (combines Module 1 DQ + Module 2 validations) ---
    st.markdown("---")
    if st.button("🟢 Run Data Quality Check"):
        # Use df from session
        df = st.session_state["df"]

        # MODULE 1: Evaluate constraints (Not Null, Regex, Values Allowed/Not Allowed, Min/Max)
        dq_results=[]
        for col,cons in st.session_state["constraints"].items():
            for c in cons:
                failed=None
                t = normalize_type_for_compare(c["type"])
                if t == normalize_type_for_compare("Not Null"):
                    failed=int(df[col].isnull().sum())
                elif t == normalize_type_for_compare("Regex Match"):
                    try:
                        pattern=re.compile(c["value"])
                        failed_series = ~df[col].astype(str).str.match(pattern,na=False)
                        failed=int(failed_series.sum())
                    except Exception:
                        failed="REGEX_ERROR"
                elif t == normalize_type_for_compare("Values Allowed (comma-separated)"):
                    allowed=[v.strip() for v in str(c["value"]).split(",")]
                    if pd.api.types.is_integer_dtype(df[col]):
                        try:
                            allowed_parsed = [int(x) for x in allowed if x!=""]
                            failed_series = ~df[col].isin(allowed_parsed)
                            failed=int(failed_series.sum())
                        except Exception:
                            failed="PARSE_ERROR"
                    else:
                        failed_series = ~df[col].astype(str).isin(allowed)
                        failed=int(failed_series.sum())
                elif t == normalize_type_for_compare("Values Not Allowed (comma-separated)"):
                    not_allowed=[v.strip() for v in str(c["value"]).split(",")]
                    if pd.api.types.is_integer_dtype(df[col]):
                        try:
                            not_allowed_parsed = [int(x) for x in not_allowed if x!=""]
                            failed_series = df[col].isin(not_allowed_parsed)
                            failed=int(failed_series.sum())
                        except Exception:
                            failed="PARSE_ERROR"
                    else:
                        failed_series = df[col].astype(str).isin(not_allowed)
                        failed=int(failed_series.sum())
                elif t == normalize_type_for_compare("Min Value"):
                    try:
                        if pd.api.types.is_datetime64_any_dtype(df[col]):
                            if isinstance(c["value"], (date, datetime)):
                                threshold = pd.to_datetime(c["value"])
                            else:
                                threshold = pd.to_datetime(str(c["value"]))
                            failed = int((df[col] < threshold).sum())
                        else:
                            failed = int((df[col] < float(c["value"])).sum())
                    except Exception:
                        failed = "ERR"
                elif t == normalize_type_for_compare("Max Value"):
                    try:
                        if pd.api.types.is_datetime64_any_dtype(df[col]):
                            if isinstance(c["value"], (date, datetime)):
                                threshold = pd.to_datetime(c["value"])
                            else:
                                threshold = pd.to_datetime(str(c["value"]))
                            failed = int((df[col] > threshold).sum())
                        else:
                            failed = int((df[col] > float(c["value"])).sum())
                    except Exception:
                        failed = "ERR"
                else:
                    failed = "SKIPPED"

                dq_results.append({
                    "Column":col,
                    "Constraint":c["type"],
                    "Value":c["value"],
                    "Failed Rows":failed
                })

        dq_df=pd.DataFrame(dq_results)
        st.subheader("✅ Data Quality Check Summary (Constraints)")
        st.dataframe(dq_df, use_container_width=True)

        # MODULE 2: Completeness & Validation (detects other validation issues, timeliness, etc.)
        # Recompute df (in case dtype conversions were applied)
        df = st.session_state["df"]
        
        # Completeness
        missing_mask = build_missing_mask(df)
        completeness_counts = (missing_mask.sum()).sort_values(ascending=False)
        completeness_report = pd.DataFrame({"Missing/Empty Count": completeness_counts})

        # Build a constraints_df from st.session_state["constraints"] (dict -> tabular)
        constraints_records = []
        for col, cons in st.session_state["constraints"].items():
            for c in cons:
                constraints_records.append({
                    "column_name": col,
                    "constraint_type": c.get("type", ""),
                    "value": c.get("value", ""),
                    "source": c.get("source", "")
                })
        constraints_df = pd.DataFrame(constraints_records) if constraints_records else pd.DataFrame(columns=["column_name","constraint_type","value","source"])

        # Validation (dynamic constraints)
        validation_report, validation_mask, all_results_df = run_validation(df, constraints_df)

        # combine masks for highlighting
        anomaly_mask = validation_mask.reindex(index=df.index, columns=df.columns, fill_value=False) | missing_mask

        # styles for preview
        def cell_style(val, row_idx, col_name):
            if missing_mask.loc[row_idx, col_name]:
                return "background-color: #ff4d4d; color: white;"
            if anomaly_mask.loc[row_idx, col_name]:
                return "background-color: #ffd633; color: black;"
            return ""

        styled_df = df.style.apply(
            lambda row: [cell_style(row[col], row.name, col) for col in df.columns],
            axis=1
        )

        # KPIs
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Rows", len(df))
        col2.metric("Columns", len(df.columns))
        col3.metric("Missing Values", int(missing_mask.sum().sum()))

        # Completeness Report
        with st.expander("🔍 Completeness Report", expanded=True):
            st.dataframe(completeness_report, use_container_width=True)
            if not completeness_report.empty:
                fig = px.bar(completeness_report, y="Missing/Empty Count", x=completeness_report.index,
                             text="Missing/Empty Count")
                st.plotly_chart(fig, use_container_width=True)

        # Validation Report
        with st.expander("✅ Validation Report", expanded=True):
            if validation_report.empty:
                st.success("No validation issues found ✅")
            else:
                st.dataframe(validation_report, use_container_width=True)
                try:
                    fig = px.bar(validation_report, x="Column", y="Invalid Count", color="Constraint", text="Invalid Count")
                    st.plotly_chart(fig, use_container_width=True)
                except Exception:
                    # fallback to simple bar chart if grouping fails
                    fig = px.bar(validation_report, x="Column", y="Invalid Count", text="Invalid Count")
                    st.plotly_chart(fig, use_container_width=True)
            
        # DQ Score (simple heuristic)
        total_cells = len(df) * len(df.columns)
        missing_total = missing_mask.sum().sum()
        invalid_total = validation_mask.sum().sum()
        
        # consider constraint failures too (sum of failed rows for numeric failures)
        try:
            constraint_fail_total = sum([r["Failed Rows"] for r in dq_results if isinstance(r["Failed Rows"], int)])
        except Exception:
            constraint_fail_total = 0
        bad = missing_total + invalid_total + constraint_fail_total
        valid_total = max(total_cells - bad, 0)
        dq_score = round((valid_total / total_cells) * 100, 2) if total_cells > 0 else 0

        # Donut chart display (animated)
        st.header("📈 Overall Data Health")
        accuracy = int(round(dq_score))
        def render_donut(value):
            fig = go.Figure(data=[go.Pie(
                values=[value, 100 - value],
                hole=0.7,
                textinfo="none",
                sort=False,
                direction="clockwise",
                rotation=0
            )])
            fig.update_layout(
                annotations=[dict(
                    text=f"<b>{value}%</b><br>Data Health",
                    x=0.5, y=0.5,
                    font_size=22, showarrow=False
                )],
                showlegend=False,
                margin=dict(t=10, b=10, l=10, r=10),
                height=350,
            )
            return fig

        donut_placeholder = st.empty()
        # animate somewhat quickly from 0 -> accuracy
        step = max(1, int(max(1, accuracy / 25)))
        for i in range(0, accuracy + 1, step):
            fig = render_donut(i)
            donut_placeholder.plotly_chart(fig, use_container_width=True)
            time.sleep(0.02)
        # ensure final exact value displayed
        donut_placeholder.plotly_chart(render_donut(accuracy), use_container_width=True, key="final")

        # Final Data Preview with highlights
        with st.expander("📑 Data Preview with Highlights", expanded=True):
            st.dataframe(styled_df, use_container_width=True)

        # Optionally display module1 dq_df again or allow export
        st.markdown("---")
        st.caption("You can export the validation reports using the Streamlit download widgets as needed.")

