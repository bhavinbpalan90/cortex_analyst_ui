"""
Cortex Analyst Chat -- A Streamlit app for conversing with Snowflake Cortex Analyst
via semantic views, using external browser (SSO) authentication.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import snowflake.connector
import streamlit as st

# ---------------------------------------------------------------------------
# Page config -- must be first Streamlit call
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Cortex Analyst",
    page_icon="snowflake",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Custom CSS for a polished, classy look
# ---------------------------------------------------------------------------
st.markdown(
    """
    <style>
    /* Sidebar header styling */
    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] h1 {
        font-size: 1.3rem;
        letter-spacing: 0.03em;
    }
    /* Chat message containers */
    [data-testid="stChatMessage"] {
        border-radius: 12px;
        padding: 0.75rem 1rem;
    }
    /* Dataframe inside chat */
    [data-testid="stChatMessage"] [data-testid="stDataFrame"] {
        border-radius: 8px;
    }
    /* Code blocks in chat */
    [data-testid="stChatMessage"] pre {
        border-radius: 8px;
    }
    /* Subtle divider between sidebar sections */
    .sidebar-divider {
        border-top: 1px solid rgba(250, 250, 250, 0.1);
        margin: 1rem 0;
    }
    /* Connected badge */
    .connected-badge {
        display: inline-block;
        background-color: #21c354;
        color: white;
        padding: 0.15rem 0.6rem;
        border-radius: 999px;
        font-size: 0.8rem;
        font-weight: 600;
        letter-spacing: 0.02em;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Session state defaults
# ---------------------------------------------------------------------------
st.session_state.setdefault("conn", None)
st.session_state.setdefault("host", None)
st.session_state.setdefault("token", None)
st.session_state.setdefault("account", "")
st.session_state.setdefault("user", "")
st.session_state.setdefault("semantic_views", [])
st.session_state.setdefault("warehouses", [])
st.session_state.setdefault("selected_warehouse", None)
st.session_state.setdefault("selected_view", None)
st.session_state.setdefault("messages", [])
st.session_state.setdefault("phase", "connect")  # connect | select | chat


# ---------------------------------------------------------------------------
# Helper: Snowflake connection
# ---------------------------------------------------------------------------
def connect_to_snowflake(account: str, user: str) -> None:
    """Establish a Snowflake connection using external browser auth."""
    try:
        conn = snowflake.connector.connect(
            account=account,
            user=user,
            authenticator="externalbrowser",
        )
        host = f"{account}.snowflakecomputing.com"
        token = conn.rest.token

        st.session_state.conn = conn
        st.session_state.host = host
        st.session_state.token = token
        st.session_state.account = account
        st.session_state.user = user
        st.session_state.phase = "select"

        # Persist credentials in URL so a browser refresh can auto-reconnect
        st.query_params["account"] = account
        st.query_params["user"] = user

        st.toast("Connected successfully")
    except Exception as exc:
        st.error(f"Connection failed: {exc}")


# ---------------------------------------------------------------------------
# Auto-reconnect: if URL has credentials but session lost the connection
# ---------------------------------------------------------------------------
def _try_auto_reconnect() -> None:
    """Re-establish connection from URL query params after a page refresh."""
    if st.session_state.conn is not None:
        return  # already connected
    qp_account = st.query_params.get("account", "")
    qp_user = st.query_params.get("user", "")
    if qp_account and qp_user:
        with st.sidebar:
            with st.spinner("Reconnecting..."):
                connect_to_snowflake(qp_account, qp_user)


_try_auto_reconnect()


# ---------------------------------------------------------------------------
# Helper: Discover semantic views
# ---------------------------------------------------------------------------
@st.cache_data(ttl=300, show_spinner=False)
def fetch_semantic_views(_conn: Any) -> list[str]:
    """Run SHOW SEMANTIC VIEWS IN ACCOUNT and return fully-qualified names."""
    cur = _conn.cursor()
    try:
        cur.execute("SHOW SEMANTIC VIEWS IN ACCOUNT")
        rows = cur.fetchall()
        desc = [col[0] for col in cur.description]
        db_idx = desc.index("database_name")
        schema_idx = desc.index("schema_name")
        name_idx = desc.index("name")
        return [f"{r[db_idx]}.{r[schema_idx]}.{r[name_idx]}" for r in rows]
    except Exception as exc:
        st.error(f"Failed to fetch semantic views: {exc}")
        return []
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# Helper: Discover warehouses
# ---------------------------------------------------------------------------
@st.cache_data(ttl=300, show_spinner=False)
def fetch_warehouses(_conn: Any) -> list[str]:
    """Run SHOW WAREHOUSES and return warehouse names."""
    cur = _conn.cursor()
    try:
        cur.execute("SHOW WAREHOUSES")
        rows = cur.fetchall()
        desc = [col[0] for col in cur.description]
        name_idx = desc.index("name")
        return [r[name_idx] for r in rows]
    except Exception as exc:
        st.error(f"Failed to fetch warehouses: {exc}")
        return []
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# Helper: Set active warehouse
# ---------------------------------------------------------------------------
def set_warehouse(warehouse: str) -> None:
    """Execute USE WAREHOUSE on the connection."""
    conn = st.session_state.conn
    if conn is None:
        return
    cur = conn.cursor()
    try:
        cur.execute(f'USE WAREHOUSE "{warehouse}"')
        st.session_state.selected_warehouse = warehouse
        st.query_params["warehouse"] = warehouse
    except Exception as exc:
        st.error(f"Failed to set warehouse: {exc}")
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# Helper: Send message to Cortex Analyst
# ---------------------------------------------------------------------------
def send_analyst_message(
    host: str,
    token: str,
    semantic_view: str,
    messages: list[dict],
) -> dict:
    """Call the Cortex Analyst REST API and return the JSON response."""
    request_body: dict[str, Any] = {
        "messages": messages,
        "semantic_view": semantic_view,
    }
    resp = requests.post(
        url=f"https://{host}/api/v2/cortex/analyst/message",
        json=request_body,
        headers={
            "Authorization": f'Snowflake Token="{token}"',
            "Content-Type": "application/json",
        },
        timeout=120,
    )
    request_id = resp.headers.get("X-Snowflake-Request-Id", "")
    if resp.status_code < 400:
        return {**resp.json(), "request_id": request_id}
    raise Exception(
        f"Request failed (id: {request_id}) "
        f"with status {resp.status_code}: {resp.text}"
    )


# ---------------------------------------------------------------------------
# Helper: Execute SQL from analyst response
# ---------------------------------------------------------------------------
def run_sql(sql: str) -> pd.DataFrame | None:
    """Execute a SQL statement and return results as a DataFrame."""
    conn = st.session_state.conn
    if conn is None:
        return None
    cur = conn.cursor()
    try:
        cur.execute(sql)
        cols = [col[0] for col in cur.description]
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)
    except Exception as exc:
        st.error(f"SQL execution error: {exc}")
        return None
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# Helper: Render analyst response content
# ---------------------------------------------------------------------------
def render_content(content_blocks: list[dict], history_mode: bool = False) -> None:
    """Render a list of Cortex Analyst content blocks (text, sql, suggestions).

    When history_mode is True, suggestions are displayed as plain text
    (not interactive) to avoid duplicate widget key errors on rerun.
    """
    for block in content_blocks:
        block_type = block.get("type", "")
        if block_type == "text":
            st.markdown(block.get("text", ""))
        elif block_type == "sql":
            sql = block.get("statement", "")
            with st.expander("Generated SQL", expanded=False):
                st.code(sql, language="sql")
            df = run_sql(sql)
            if df is not None and not df.empty:
                st.dataframe(df, use_container_width=True)
        elif block_type == "suggestions":
            suggestions = block.get("suggestions", [])
            if suggestions:
                st.caption("Suggested follow-ups")
                if history_mode:
                    for s in suggestions:
                        st.markdown(f"- {s}")
                else:
                    for i, s in enumerate(suggestions):
                        if st.button(s, key=f"suggestion_{len(st.session_state.messages)}_{i}"):
                            handle_user_question(s)


# ---------------------------------------------------------------------------
# Handle new user question
# ---------------------------------------------------------------------------
def handle_user_question(question: str) -> None:
    """Append user question, call API, render response, update history."""
    # Ensure alternating roles -- if last message is already 'user', remove it
    # to avoid consecutive user messages (API requires strict alternation).
    if st.session_state.messages and st.session_state.messages[-1]["role"] == "user":
        st.session_state.messages.pop()

    user_msg = {
        "role": "user",
        "content": [{"type": "text", "text": question}],
    }
    st.session_state.messages.append(user_msg)

    with st.chat_message("user"):
        st.markdown(question)

    # Build API payload -- include full history for multi-turn
    with st.chat_message("assistant"):
        with st.spinner("Analyzing..."):
            try:
                result = send_analyst_message(
                    host=st.session_state.host,
                    token=st.session_state.token,
                    semantic_view=st.session_state.selected_view,
                    messages=st.session_state.messages,
                )
                analyst_content = result.get("message", {}).get("content", [])
                render_content(analyst_content)

                # Save analyst response in history
                analyst_msg = {
                    "role": "analyst",
                    "content": analyst_content,
                }
                st.session_state.messages.append(analyst_msg)
            except Exception as exc:
                # Remove the user message so history stays valid for next attempt
                if st.session_state.messages and st.session_state.messages[-1]["role"] == "user":
                    st.session_state.messages.pop()
                st.error(f"Analyst error: {exc}")


# ===========================================================================
# SIDEBAR
# ===========================================================================
with st.sidebar:
    st.markdown("# Cortex Analyst")
    st.caption("Chat with your data using natural language")
    st.markdown('<div class="sidebar-divider"></div>', unsafe_allow_html=True)

    # ---- Connection form ----
    with st.container():
        st.subheader("Connection")
        if st.session_state.phase == "connect":
            with st.form("connection_form"):
                account = st.text_input(
                    "Account name",
                    value=st.query_params.get("account", ""),
                    placeholder="orgname-acctname",
                    help="Your Snowflake account identifier, e.g. myorg-myaccount",
                )
                user = st.text_input(
                    "User name",
                    value=st.query_params.get("user", ""),
                    placeholder="you@company.com",
                    help="Your Snowflake login user name",
                )
                connect_btn = st.form_submit_button(
                    "Connect",
                    use_container_width=True,
                    type="primary",
                )
                if connect_btn:
                    if account and user:
                        connect_to_snowflake(account.strip(), user.strip())
                        st.rerun()
                    else:
                        st.warning("Both fields are required.")
        else:
            st.markdown(
                '<span class="connected-badge">Connected</span>',
                unsafe_allow_html=True,
            )
            st.caption(f"**Account:** {st.session_state.host}")
            if st.button("Disconnect", use_container_width=True):
                try:
                    st.session_state.conn.close()
                except Exception:
                    pass
                for key in ("conn", "host", "token", "selected_view", "selected_warehouse"):
                    st.session_state[key] = None
                for key in ("semantic_views", "messages", "warehouses"):
                    st.session_state[key] = []
                st.session_state.account = ""
                st.session_state.user = ""
                st.session_state.phase = "connect"
                st.query_params.clear()
                st.rerun()

    st.markdown('<div class="sidebar-divider"></div>', unsafe_allow_html=True)

    # ---- Warehouse picker ----
    if st.session_state.phase in ("select", "chat"):
        st.subheader("Warehouse")

        if not st.session_state.warehouses:
            with st.spinner("Loading warehouses..."):
                whs = fetch_warehouses(st.session_state.conn)
                st.session_state.warehouses = whs

        whs = st.session_state.warehouses
        if whs:
            # Determine default index from saved selection or query param
            saved_wh = st.session_state.selected_warehouse or st.query_params.get("warehouse", "")
            default_idx = whs.index(saved_wh) if saved_wh in whs else 0

            chosen_wh = st.selectbox(
                "Choose a warehouse",
                whs,
                index=default_idx,
                label_visibility="collapsed",
                key="warehouse_select",
            )
            # Set warehouse if changed or not yet set
            if chosen_wh != st.session_state.selected_warehouse:
                set_warehouse(chosen_wh)
        else:
            st.info("No warehouses found.")

    st.markdown('<div class="sidebar-divider"></div>', unsafe_allow_html=True)

    # ---- Semantic view picker ----
    if st.session_state.phase in ("select", "chat"):
        st.subheader("Semantic view")

        if not st.session_state.semantic_views:
            with st.spinner("Discovering semantic views..."):
                views = fetch_semantic_views(st.session_state.conn)
                st.session_state.semantic_views = views

        views = st.session_state.semantic_views
        if views:
            chosen_view = st.selectbox(
                "Choose a semantic view",
                views,
                index=views.index(st.session_state.selected_view) if st.session_state.selected_view in views else 0,
                label_visibility="collapsed",
            )
            if st.session_state.phase == "select":
                if st.button("Start chatting", use_container_width=True, type="primary"):
                    st.session_state.selected_view = chosen_view
                    st.session_state.messages = []
                    st.session_state.phase = "chat"
                    st.rerun()
            else:
                if chosen_view != st.session_state.selected_view:
                    st.session_state.selected_view = chosen_view
                    st.session_state.messages = []
                    st.rerun()
        else:
            st.info("No semantic views found in this account.")

    st.markdown('<div class="sidebar-divider"></div>', unsafe_allow_html=True)

    # ---- About ----
    with st.expander("About"):
        st.caption(
            "This app connects to Snowflake using external browser (SSO) "
            "authentication, discovers semantic views, and lets you have a "
            "natural-language conversation with your data via Cortex Analyst."
        )


# ===========================================================================
# MAIN AREA
# ===========================================================================
if st.session_state.phase == "connect":
    # Welcome / landing screen
    st.markdown(
        """
        <div style="display: flex; flex-direction: column; align-items: center;
                    justify-content: center; padding: 4rem 1rem; text-align: center;">
            <h1 style="font-size: 2.5rem; font-weight: 700; margin-bottom: 0.5rem;">
                Cortex Analyst
            </h1>
            <p style="font-size: 1.1rem; opacity: 0.7; max-width: 520px;">
                Chat with your Snowflake data using natural language.
                Connect your account to get started.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

elif st.session_state.phase == "select":
    st.markdown(
        """
        <div style="display: flex; flex-direction: column; align-items: center;
                    justify-content: center; padding: 4rem 1rem; text-align: center;">
            <h2 style="font-weight: 600; margin-bottom: 0.5rem;">
                Select a semantic view
            </h2>
            <p style="opacity: 0.7; max-width: 480px;">
                Pick a semantic view from the sidebar to start asking questions about your data.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

elif st.session_state.phase == "chat":
    # ---- Render chat history ----
    for msg in st.session_state.messages:
        role = msg["role"]
        if role == "user":
            with st.chat_message("user"):
                for block in msg.get("content", []):
                    if block.get("type") == "text":
                        st.markdown(block["text"])
        elif role == "analyst":
            with st.chat_message("assistant"):
                render_content(msg.get("content", []), history_mode=True)

    # ---- Chat input ----
    if prompt := st.chat_input("Ask a question about your data..."):
        handle_user_question(prompt)
