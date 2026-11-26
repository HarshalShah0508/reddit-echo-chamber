"""Public Streamlit front-end for reddit_kolkata.query_interface.

Lets anyone ask a natural-language question about the r/kolkata, r/KolkataCity, and
r/KolkataLife dataset; Gemini writes the DuckDB SQL, this app validates and runs it.

Run locally:
    streamlit run app.py

Deploy: push this repo (including reddit_kolkata/parquet/ and the processed/*.csv files
it reads) to Streamlit Community Cloud, pointing at this file, with GEMINI_API_KEY set
under the app's Secrets (not committed to .env).
"""
from __future__ import annotations

import os

import streamlit as st

from reddit_kolkata.query_interface import _connect, generate_sql, validate_readonly

# Public deployments use a shared API key paid for by the owner, not the visitor --
# cap queries per browser session so one visitor can't exhaust the Gemini quota/budget
# for everyone else. Raise this if you're fine with the exposure.
MAX_QUERIES_PER_SESSION = 15

EXAMPLE_QUESTIONS = [
    "How many posts and comments does each subreddit have in total?",
    "Top 10 highest-scoring posts in kolkatalife",
    "Which month had the most posts across all three subreddits?",
    "How many unique authors posted in both kolkata and kolkatacity?",
]

st.set_page_config(page_title="Reddit Kolkata Query", page_icon="🔎", layout="centered")


@st.cache_resource(show_spinner=False)
def get_connection():
    return _connect()


def _streamlit_secret_as_env() -> None:
    """Streamlit Cloud injects secrets via st.secrets, not a .env file -- mirror the key
    into os.environ so query_interface's genai.Client() (which reads GEMINI_API_KEY from
    the environment) finds it the same way it would locally."""
    if "GEMINI_API_KEY" in st.secrets and not os.environ.get("GEMINI_API_KEY"):
        os.environ["GEMINI_API_KEY"] = st.secrets["GEMINI_API_KEY"]


_streamlit_secret_as_env()

st.title("🔎 Reddit Kolkata Query")
st.caption(
    "Ask a question in plain English about r/kolkata, r/KolkataCity, or r/KolkataLife. "
    "Gemini writes a read-only SQL query against the collected dataset and runs it live."
)

if "query_count" not in st.session_state:
    st.session_state.query_count = 0

with st.expander("Example questions"):
    for q in EXAMPLE_QUESTIONS:
        if st.button(q, use_container_width=True):
            st.session_state.question = q

question = st.text_input(
    "Your question",
    key="question",
    placeholder="e.g. How many posts mention metro per month?",
)
effort = st.selectbox("Effort", ["low", "medium", "high", "xhigh", "max"], index=0)
ask = st.button("Ask", type="primary")

if ask and question.strip():
    if st.session_state.query_count >= MAX_QUERIES_PER_SESSION:
        st.error(
            f"This demo caps each session at {MAX_QUERIES_PER_SESSION} questions to keep "
            "the shared API quota available for everyone. Refresh the page for a new session."
        )
    else:
        st.session_state.query_count += 1
        with st.spinner("Asking Gemini..."):
            try:
                sql, notes = generate_sql(question, effort=effort)
            except SystemExit as e:
                st.error(str(e))
                sql = None

        if sql:
            try:
                validate_readonly(sql)
            except ValueError as e:
                st.error(f"Generated query rejected: {e}")
                sql = None

        if sql:
            st.code(sql.strip(), language="sql")
            if notes:
                st.info(notes)
            try:
                con = get_connection()
                df = con.execute(sql).fetchdf()
                if df.empty:
                    st.write("(0 rows)")
                else:
                    st.dataframe(df, use_container_width=True)
            except Exception as e:
                st.error(f"Query failed: {e}")

st.caption(f"Queries this session: {st.session_state.query_count}/{MAX_QUERIES_PER_SESSION}")
