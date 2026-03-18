# Cortex Analyst UI (Streamlit)

Streamlit UI that connects to **Snowflake** using **External Browser (SSO)** authentication, discovers **Semantic Views**, and chats with **Cortex Analyst** via the REST API (`/api/v2/cortex/analyst/message`). The app can optionally execute generated SQL back in Snowflake and render results.

## Prerequisites

- Python **3.8+** (recommended: 3.10+)
- Network access to your Snowflake account
- A Snowflake user that can authenticate via **external browser** (SSO)
- Access to semantic views (the app runs `SHOW SEMANTIC VIEWS IN ACCOUNT`)
- Ability to use a warehouse (the app runs `SHOW WAREHOUSES` and `USE WAREHOUSE "<name>"`)

## Install

From the repo root:

```bash
cd CortexAnalystUI
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .
```

Dependencies are defined in `pyproject.toml`.

## Run locally

```bash
cd CortexAnalystUI
source .venv/bin/activate
streamlit run streamlit_app.py
```

Then in the sidebar:

- Enter **Account name** (e.g. `orgname-acctname`)
- Enter **User name** (e.g. `you@company.com`)
- Click **Connect** (your browser will open for SSO)
- Choose a **Warehouse**
- Choose a **Semantic view**
- Click **Start chatting**

## Notes / Troubleshooting

- **External browser auth is interactive**: it is best suited for local use on a workstation where a browser can be opened.
- **Account hostname**: the app uses `https://{account}.snowflakecomputing.com` for API calls. If your account uses a different hostname, update the `host` construction in `streamlit_app.py`.
- **Semantic views**: if `SHOW SEMANTIC VIEWS IN ACCOUNT` returns nothing, confirm your role/privileges and that semantic views exist in the account.
- **Generated SQL execution**: when Cortex Analyst returns SQL, the app executes it on your current connection and renders a dataframe (if non-empty).

## Deployment

### Option A: Single-user “run it on a box” (recommended with `externalbrowser`)

Run the Streamlit server on the same machine where the user can complete the SSO browser flow:

```bash
cd CortexAnalystUI
source .venv/bin/activate
streamlit run streamlit_app.py --server.address 0.0.0.0 --server.port 8501
```

If exposing beyond localhost, put it behind your standard reverse proxy / SSO controls.

### Option B: Shared / headless deployment (requires auth change)

`authenticator="externalbrowser"` generally **does not work well for headless/multi-user server deployments** (the login flow is tied to an interactive browser session on the server).

If you need a shared deployment, switch the connection method in `connect_to_snowflake()` to a server-friendly auth mechanism (for example: OAuth, key-pair, or another approved enterprise pattern), and then deploy Streamlit normally (container, VM, Kubernetes, etc.).

## Repo hygiene

This folder includes a `.gitignore` that excludes `.venv/` and `.streamlit/secrets.toml`. Avoid committing virtual environments.

