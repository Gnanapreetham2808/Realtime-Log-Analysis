import re
import os
from datetime import datetime

import pandas as pd
import streamlit as st


LOG_PATTERN = re.compile(r"(\S+) (\S+) (\S+) \[(.*?)\] \"(.*?)\" (\d{3}) (\S+)")


def parse_log_line(line: str):
    m = LOG_PATTERN.match(line)
    if not m:
        return None
    ip = m.group(1)
    client = m.group(2)
    user = m.group(3)
    timestamp_raw = m.group(4)  # e.g. 10/Oct/2000:13:55:36 -0700
    request = m.group(5)
    status = m.group(6)
    size = m.group(7)

    # Parse timestamp
    dt = None
    for fmt in ("%d/%b/%Y:%H:%M:%S %z", "%d/%b/%Y:%H:%M:%S"):
        try:
            dt = datetime.strptime(timestamp_raw, fmt)
            break
        except Exception:
            continue

    return {
        "ip": ip,
        "client": client,
        "user": user,
        "timestamp": dt,
        "request": request,
        "status": int(status) if status.isdigit() else None,
        "size": int(size) if size.isdigit() else (0 if size == "-" else None),
    }


@st.cache_data
def load_log_from_path(path: str) -> pd.DataFrame:
    rows = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            parsed = parse_log_line(line)
            if parsed:
                rows.append(parsed)
    df = pd.DataFrame(rows)
    # If timestamps are missing tz info, assume local naive and convert to pandas datetime
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])  # handles None
    return df


@st.cache_data
def load_log_from_bytes(uploaded_bytes) -> pd.DataFrame:
    text = uploaded_bytes.decode("utf-8", errors="ignore")
    rows = []
    for line in text.splitlines():
        parsed = parse_log_line(line)
        if parsed:
            rows.append(parsed)
    df = pd.DataFrame(rows)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def main_ui():
    st.title("Real-time Log Analysis — Streamlit")
    st.markdown("Upload an Apache/Nginx access log or pick the default sample path in the repo.")

    # Sidebar options
    st.sidebar.header("Data source")
    uploaded_file = st.sidebar.file_uploader("Upload log file", type=["log", "txt"]) 
    use_repo_path = False
    repo_default = os.path.join(os.getcwd(), "logdata", "web-server-access-logs_10k.log")
    if os.path.exists(repo_default):
        if st.sidebar.checkbox("Use repository log file (default)", value=True):
            use_repo_path = True

    # Load data
    df = None
    if uploaded_file is not None:
        df = load_log_from_bytes(uploaded_file.getvalue())
        st.sidebar.success("Loaded uploaded file")
    elif use_repo_path:
        try:
            df = load_log_from_path(repo_default)
            st.sidebar.success(f"Loaded {repo_default}")
        except FileNotFoundError:
            st.sidebar.error("Repo log file not found")
    else:
        st.info("No log loaded. Upload a file or enable 'Use repository log file' in the sidebar.")

    if df is None or df.empty:
        st.stop()

    st.write(f"Loaded {len(df)} parsed log lines")

    # Ensure timestamp index
    if df["timestamp"].isna().all():
        st.error("No parsable timestamps found in the log. Ensure your log uses common CLF format with timestamps like 10/Oct/2000:13:55:36 -0700.")
        st.stop()

    # Let user pick time range
    # Convert pandas.Timestamp to native python datetimes for Streamlit slider compatibility
    min_ts = pd.to_datetime(df["timestamp"].min()).to_pydatetime()
    max_ts = pd.to_datetime(df["timestamp"].max()).to_pydatetime()
    start, end = st.sidebar.slider("Select time range", value=(min_ts, max_ts), min_value=min_ts, max_value=max_ts)
    mask = (df["timestamp"] >= pd.to_datetime(start)) & (df["timestamp"] <= pd.to_datetime(end))
    df = df.loc[mask]

    # Request volume over time
    st.header("Request volume over time")
    resample_choice = st.selectbox("Resample frequency", options=["1T", "5T", "15T", "1H"], index=1, format_func=lambda x: {"1T":"1 minute","5T":"5 minutes","15T":"15 minutes","1H":"1 hour"}[x])

    series = df.set_index("timestamp").resample(resample_choice).size()
    series = series.rename("requests")
    st.line_chart(series)

    # HTTP Status Code Distribution with Counts and Percentages
    st.header("HTTP Status Code Distribution with Counts and Percentages")
    status_counts = df["status"].value_counts(dropna=True).sort_index()
    if status_counts.empty:
        st.write("No status codes found in the selected range")
    else:
        # Build a clean DataFrame: reset_index then explicitly name columns to avoid ambiguous renames
        status_df = status_counts.reset_index()
        status_df.columns = ["status", "count"]
        # Ensure numeric types
        status_df["count"] = status_df["count"].astype(int)
        status_df["percent"] = status_df["count"] / status_df["count"].sum() * 100
        # Convert status to string for display
        status_df["status"] = status_df["status"].astype(int).astype(str)

        st.bar_chart(status_df.set_index("status")["count"])

        st.write("### Counts and percentages")
        st.dataframe(status_df.style.format({"percent":"{:.1f}%"}))

    # Refresh control: allow user to refresh the loaded data (useful if files are added to the folder)
    if st.sidebar.button("Refresh data"):
        st.experimental_rerun()

    # Additional details: top IPs and requests
    st.header("Top IPs and Requests")
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Top IPs")
        top_ips = df["ip"].value_counts().head(10)
        st.table(top_ips)
    with col2:
        st.subheader("Top Requests")
        top_reqs = df["request"].value_counts().head(10)
        st.table(top_reqs)

    st.markdown("---")
    st.caption("If plots look empty, try uploading a log file or expanding the time range.")


if __name__ == "__main__":
    main_ui()
