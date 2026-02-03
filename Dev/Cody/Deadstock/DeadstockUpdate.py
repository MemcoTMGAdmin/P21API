pip install requests pyodbc

import argparse
import csv
import json
import logging
import random
import smtplib
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import pyodbc


# -------------------- Config / Constants --------------------

ENV_NAME = "PROD"
P21_API_BASE = "https://themiddletongroup-play-api.epicordistribution.com"
TOKEN_URL = f"{P21_API_BASE}/api/security/token/v2"
TRANSACTION_URL = f"{P21_API_BASE}/uiserver0/api/v2/transaction"

API_MAX_ATTEMPTS = 4
API_BASE_DELAY_MS = 1000
API_MAX_DELAY_MS = 8000
API_TIMEOUT_SEC = 120

SMTP_SERVER = "mail.smtp2go.com"
SMTP_PORT = 587
MAIL_FROM = "IT@themiddletongroup.com"
MAIL_TO = "italerts@tmgprivate.com"
SMTP_USER = "ITScript"

SQL_SERVER = "p21us-read06.epicordistribution.com,50135"
SQL_DATABASE = "az_130611_live"
SQL_USER = "readonly_130611_live"

REQUIRED_SECRET_FILES = [
    Path("./sqlpwd.txt"),
    Path("./smtp2go_pwd.txt"),
]

# Your T-SQL verbatim (note: TOP (1) will only return one row)
TSQL = r""DECLARE @today_utc date = CAST(SYSUTCDATETIME() AS date);
DECLARE @d45       date = DATEADD(DAY, -45, @today_utc);
DECLARE @d90       date = DATEADD(DAY, -90, @today_utc);

WITH qty_by_item AS (
    SELECT
        im.item_id,
        COALESCE(SUM(iloc.qty_on_hand), 0) AS qty
    FROM dbo.inv_mast im
    LEFT JOIN dbo.inv_loc iloc
        ON iloc.inv_mast_uid = im.inv_mast_uid
    GROUP BY
        im.item_id
),
last_invoice_by_item AS (
    SELECT
        im.item_id,
        MAX(il.date_created) AS last_invoiced
    FROM dbo.inv_mast im
    LEFT JOIN dbo.invoice_line il
        ON il.inv_mast_uid = im.inv_mast_uid
    GROUP BY
        im.item_id
),
first_receipt_by_item AS (
    SELECT
        im.item_id,
        MIN(CASE WHEN pl.received_date IS NOT NULL THEN pl.received_date END) AS first_received
    FROM dbo.inv_mast im
    LEFT JOIN dbo.po_line pl
        ON pl.inv_mast_uid = im.inv_mast_uid
    GROUP BY
        im.item_id
)
SELECT
    im.item_id,
    im.inv_mast_uid,
    q.qty,
    li.last_invoiced,
    im.class_id2,
    fr.first_received,
    CASE
        /* 1) Seasonal override (human-curated wins, normalize to allowed label) */
        WHEN im.class_id2 IS NOT NULL
         AND LOWER(im.class_id2) LIKE '%seas%' THEN 'Seasonal Item'

        /* 2) No stock on hand */
        WHEN q.qty = 0 THEN NULL

        /* 3) Was DEAD, but sold within last 90 days -> still Dead Stock (keep branch for future) */
        WHEN im.class_id2 IS NOT NULL
         AND LOWER(im.class_id2) LIKE '%dead%'
         AND li.last_invoiced IS NOT NULL
         AND CAST(li.last_invoiced AS date) >= @d90 THEN 'Dead Stock'

        /* 4) Dead Stock (independent of current class), stocked & stale */
        WHEN q.qty > 0 AND (
               (li.last_invoiced IS NOT NULL AND CAST(li.last_invoiced AS date) < @d90)
            OR (li.last_invoiced IS NULL     AND fr.first_received IS NOT NULL AND CAST(fr.first_received AS date) < @d90)
        ) THEN 'Dead Stock'

        /* 5) 45 Day Inactive Item: 46–90 day quiet window, only if currently unclassified */
        WHEN q.qty > 0
         AND im.class_id2 IS NULL
         AND li.last_invoiced IS NOT NULL
         AND CAST(li.last_invoiced AS date) <  @d45
         AND CAST(li.last_invoiced AS date) >= @d90 THEN '45 Day Inactive Item'

        /* 6) Otherwise active/normal */
        ELSE NULL
    END AS status
FROM dbo.inv_mast im
LEFT JOIN qty_by_item           q  ON q.item_id  = im.item_id
LEFT JOIN last_invoice_by_item  li ON li.item_id = im.item_id
LEFT JOIN first_receipt_by_item fr ON fr.item_id = im.item_id

/* ---- Return only rows that actually need updating ----*/
WHERE NOT (
    (im.class_id2 IS NULL AND
     CASE
        WHEN im.class_id2 IS NOT NULL AND LOWER(im.class_id2) LIKE '%seas%' THEN 'Seasonal Item'
        WHEN q.qty = 0 THEN NULL
        WHEN im.class_id2 IS NOT NULL AND LOWER(im.class_id2) LIKE '%dead%' AND li.last_invoiced IS NOT NULL AND CAST(li.last_invoiced AS date) >= @d90 THEN 'Dead Stock'
        WHEN q.qty > 0 AND (
               (li.last_invoiced IS NOT NULL AND CAST(li.last_invoiced AS date) < @d90)
            OR (li.last_invoiced IS NULL     AND fr.first_received IS NOT NULL AND CAST(fr.first_received AS date) < @d90)
        ) THEN 'Dead Stock'
        WHEN q.qty > 0 AND im.class_id2 IS NULL AND li.last_invoiced IS NOT NULL
             AND CAST(li.last_invoiced AS date) < @d45 AND CAST(li.last_invoiced AS date) >= @d90 THEN '45 Day Inactive Item'
        ELSE NULL
     END IS NULL)
 OR
    (im.class_id2 IS NOT NULL AND
     im.class_id2 = CASE
        WHEN im.class_id2 IS NOT NULL AND LOWER(im.class_id2) LIKE '%seas%' THEN 'Seasonal Item'
        WHEN q.qty = 0 THEN NULL
        WHEN im.class_id2 IS NOT NULL AND LOWER(im.class_id2) LIKE '%dead%' AND li.last_invoiced IS NOT NULL AND CAST(li.last_invoiced AS date) >= @d90 THEN 'Dead Stock'
        WHEN q.qty > 0 AND (
               (li.last_invoiced IS NOT NULL AND CAST(li.last_invoiced AS date) < @d90)
            OR (li.last_invoiced IS NULL     AND fr.first_received IS NOT NULL AND CAST(fr.first_received AS date) < @d90)
        ) THEN 'Dead Stock'
        WHEN q.qty > 0 AND im.class_id2 IS NULL AND li.last_invoiced IS NOT NULL
             AND CAST(li.last_invoiced AS date) < @d45 AND CAST(li.last_invoiced AS date) >= @d90 THEN '45 Day Inactive Item'
        ELSE NULL
     END)
)
ORDER BY
    im.item_id;""


# -------------------- Helpers --------------------

def read_text_secret(path: Path) -> str:
    # Matches your current approach: plaintext in a file.
    # Strip to remove trailing newlines.
    return path.read_text(encoding="utf-8").strip()


def preflight_secrets():
    missing = [p for p in REQUIRED_SECRET_FILES if not p.exists()]
    if missing:
        raise RuntimeError(
            "Missing required secret file(s): "
            + ", ".join(str(p) for p in missing)
            + ". Run setup-secrets first."
        )


def get_p21_token(client_secret: str, auth_url: str = TOKEN_URL) -> str:
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    body = {"ClientSecret": client_secret, "GrantType": "client_credentials"}

    try:
        r = requests.post(auth_url, headers=headers, json=body, timeout=60)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        raise RuntimeError(f"Failed to get Prophet21 token: {e}") from e

    # Mirror your flexible field name handling
    for key in ("AccessToken", "access_token", "token"):
        if key in data and data[key]:
            return data[key]

    raise RuntimeError(f"Unexpected token response format: {data}")


def run_sql_query_odbc(server: str, database: str, user: str, password: str, query: str) -> List[Dict[str, Any]]:
    # TrustServerCertificate=True matches your PS connection string behavior.
    # NOTE: driver name may need adjusting per machine.
    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server={server};Database={database};UID={user};PWD={password};"
        "TrustServerCertificate=yes;"
    )

    rows: List[Dict[str, Any]] = []
    with pyodbc.connect(conn_str, timeout=10) as conn:
        conn.timeout = 600  # command timeout-ish
        with conn.cursor() as cur:
            cur.execute(query)
            cols = [c[0] for c in cur.description]
            for r in cur.fetchall():
                rows.append(dict(zip(cols, r)))
    return rows


def build_p21_item_status_body(item_id: str, new_status: Optional[str], tx_status: str) -> Dict[str, Any]:
    return {
        "Name": "Item",
        "UseCodeValues": False,
        "IgnoreDisabled": False,
        "Transactions": [
            {
                "Status": tx_status,
                "DataElements": [
                    {
                        "Name": "TABPAGE_1.tp_1_dw_1",
                        "BusinessObjectName": None,
                        "Type": "Form",
                        "Keys": ["item_id"],
                        "Rows": [
                            {
                                "Edits": [{"Name": "item_id", "Value": item_id, "IgnoreIfEmpty": True}],
                                "RelativeDateEdits": [],
                            }
                        ],
                    },
                    {
                        "Name": "TABPAGE_CLASSES.classes",
                        "BusinessObjectName": None,
                        "Type": "Form",
                        "Keys": ["item_id"],
                        "Rows": [
                            {
                                "Edits": [{"Name": "class_id2", "Value": new_status, "IgnoreIfEmpty": False}],
                                "RelativeDateEdits": [],
                            }
                        ],
                    },
                ],
                "Documents": None,
            }
        ],
        "Query": None,
        "FieldMap": [],
        "TransactionSplitMethod": 0,
        "Parameters": None,
    }


def post_with_retry(
    url: str,
    headers: Dict[str, str],
    json_body: Dict[str, Any],
    timeout_sec: int,
    max_attempts: int,
    base_delay_ms: int,
    max_delay_ms: int,
) -> Tuple[int, Any, int]:
    attempt = 0
    while True:
        attempt += 1
        try:
            r = requests.post(url, headers=headers, json=json_body, timeout=timeout_sec, allow_redirects=False)
            status = r.status_code

            # Retryable HTTP codes similar to your PS logic
            if status in (408, 429, 500, 502, 503, 504):
                raise requests.HTTPError(f"Retryable HTTP {status}", response=r)

            r.raise_for_status()
            # Could be JSON or empty; keep robust:
            try:
                return status, r.json(), attempt
            except Exception:
                return status, r.text, attempt

        except Exception as e:
            status_code = None
            resp_body = None

            if isinstance(e, requests.HTTPError) and getattr(e, "response", None) is not None:
                status_code = e.response.status_code
                try:
                    resp_body = e.response.text
                except Exception:
                    resp_body = None

            retryable = (
                status_code is None  # network/timeout/no response
                or status_code in (408, 429, 500, 502, 503, 504)
            )

            if (not retryable) or (attempt >= max_attempts):
                msg = str(e)
                if status_code is not None:
                    msg = f"HTTP {status_code}: {msg}"
                raise RuntimeError(f"API call failed after {attempt} attempt(s). {msg}. Body={resp_body!r}") from e

            delay_ms = min(max_delay_ms, int(base_delay_ms * (2 ** (attempt - 2))))
            delay_ms += random.randint(0, 250)  # jitter
            time.sleep(delay_ms / 1000.0)


def send_log_email_smtp2go(
    smtp_server: str,
    smtp_port: int,
    username: str,
    password: str,
    mail_from: str,
    mail_to: str,
    subject: str,
    body: str,
    attachment_path: Path,
):
    if not attachment_path.exists():
        raise RuntimeError(f"Attachment not found: {attachment_path}")

    msg = EmailMessage()
    msg["From"] = mail_from
    msg["To"] = mail_to
    msg["Subject"] = subject
    msg.set_content(body)

    data = attachment_path.read_bytes()
    msg.add_attachment(data, maintype="text", subtype="csv", filename=attachment_path.name)

    with smtplib.SMTP(smtp_server, smtp_port) as
