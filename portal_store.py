import json
import os
import re
import sqlite3
from datetime import datetime, UTC, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional


PLAN_CATALOG: Dict[str, Dict[str, Any]] = {
    "free": {
        "slug": "free",
        "name": "Free Explorer",
        "tagline": "For evaluation and one-off archive tests.",
        "monthly_cents": 0,
        "yearly_cents": 0,
        "trial_days": 0,
        "limits": [
            "1 seat",
            "3 processed logs per month",
            "Manual review tools",
            "Community email support",
        ],
        "cta": "Start Free",
    },
    "pro": {
        "slug": "pro",
        "name": "Pro Digitizer",
        "tagline": "For solo interpreters and technical teams digitizing active wells.",
        "monthly_cents": 7900,
        "yearly_cents": 79000,
        "trial_days": 30,
        "limits": [
            "3 seats",
            "Unlimited processed logs",
            "Black-trace training capture",
            "Priority support",
        ],
        "cta": "Choose Pro",
    },
    "team": {
        "slug": "team",
        "name": "Team Archive",
        "tagline": "For larger archive recovery projects and shared workspaces.",
        "monthly_cents": 24900,
        "yearly_cents": 249000,
        "trial_days": 30,
        "limits": [
            "10 seats",
            "Shared admin workspace",
            "Bulk archive intake",
            "Admin billing controls",
        ],
        "cta": "Choose Team",
    },
    "enterprise": {
        "slug": "enterprise",
        "name": "Enterprise",
        "tagline": "For managed archive programs, private deployments, and custom support.",
        "monthly_cents": 0,
        "yearly_cents": 0,
        "trial_days": 30,
        "limits": [
            "Unlimited seats",
            "Private deployment options",
            "Dedicated onboarding",
            "Custom billing terms",
        ],
        "cta": "Talk to Sales",
    },
}


def _utc_now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def _utc_after_days(days: int) -> str:
    return (datetime.now(UTC) + timedelta(days=int(days))).strftime("%Y-%m-%d %H:%M:%S UTC")


def trial_days_for_plan(plan_slug: str) -> int:
    plan = PLAN_CATALOG.get(plan_slug) or PLAN_CATALOG["free"]
    return max(0, int(plan.get("trial_days") or 0))


def trial_window_for_plan(plan_slug: str) -> tuple[Optional[str], Optional[str]]:
    days = trial_days_for_plan(plan_slug)
    if days <= 0:
        return None, None
    return _utc_now(), _utc_after_days(days)


def resolve_db_path() -> Path:
    explicit = str(os.environ.get("TURBOTIFFLAS_DB_PATH") or "").strip()
    if explicit:
        path = Path(explicit).expanduser()
    else:
        volume_mount = str(os.environ.get("RAILWAY_VOLUME_MOUNT_PATH") or "").strip()
        if volume_mount:
            path = Path(volume_mount) / "portal" / "turbotifflas_portal.db"
        else:
            path = Path(__file__).resolve().parent / "data" / "turbotifflas_portal.db"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


DB_PATH = resolve_db_path()


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _row_to_dict(row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}


def init_db(admin_email: str, admin_password_hash: str, admin_name: str = "Platform Admin") -> None:
    with _connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                full_name TEXT NOT NULL,
                company TEXT,
                is_admin INTEGER NOT NULL DEFAULT 0,
                plan_slug TEXT NOT NULL DEFAULT 'free',
                billing_cycle TEXT NOT NULL DEFAULT 'monthly',
                subscription_status TEXT NOT NULL DEFAULT 'trial',
                trial_started_at TEXT,
                trial_ends_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_login_at TEXT
            );

            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                plan_slug TEXT NOT NULL,
                billing_cycle TEXT NOT NULL,
                amount_cents INTEGER NOT NULL,
                original_amount_cents INTEGER,
                discount_cents INTEGER NOT NULL DEFAULT 0,
                coupon_code TEXT,
                currency TEXT NOT NULL DEFAULT 'USD',
                provider TEXT NOT NULL DEFAULT 'sandbox',
                provider_ref TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                note TEXT,
                metadata_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS coupons (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL UNIQUE,
                description TEXT,
                discount_type TEXT NOT NULL,
                discount_value INTEGER NOT NULL DEFAULT 0,
                applies_to_plan TEXT,
                active INTEGER NOT NULL DEFAULT 1,
                max_redemptions INTEGER,
                expires_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS coupon_redemptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                coupon_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                payment_id INTEGER,
                coupon_code TEXT NOT NULL,
                discount_type TEXT NOT NULL,
                discount_value INTEGER NOT NULL DEFAULT 0,
                amount_cents_applied INTEGER NOT NULL DEFAULT 0,
                trial_days_applied INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'applied',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(coupon_id) REFERENCES coupons(id),
                FOREIGN KEY(user_id) REFERENCES users(id),
                FOREIGN KEY(payment_id) REFERENCES payments(id)
            );
            """
        )

        user_columns = {row["name"] for row in conn.execute("PRAGMA table_info(users)").fetchall()}
        if "trial_started_at" not in user_columns:
            conn.execute("ALTER TABLE users ADD COLUMN trial_started_at TEXT")
        if "trial_ends_at" not in user_columns:
            conn.execute("ALTER TABLE users ADD COLUMN trial_ends_at TEXT")

        payment_columns = {row["name"] for row in conn.execute("PRAGMA table_info(payments)").fetchall()}
        if "original_amount_cents" not in payment_columns:
            conn.execute("ALTER TABLE payments ADD COLUMN original_amount_cents INTEGER")
        if "discount_cents" not in payment_columns:
            conn.execute("ALTER TABLE payments ADD COLUMN discount_cents INTEGER NOT NULL DEFAULT 0")
        if "coupon_code" not in payment_columns:
            conn.execute("ALTER TABLE payments ADD COLUMN coupon_code TEXT")

        existing_admin = conn.execute(
            "SELECT id FROM users WHERE lower(email) = lower(?)",
            (admin_email,),
        ).fetchone()
        if existing_admin is None:
            now = _utc_now()
            conn.execute(
                """
                INSERT INTO users (
                    email, password_hash, full_name, company, is_admin,
                    plan_slug, billing_cycle, subscription_status, trial_started_at, trial_ends_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    admin_email.strip().lower(),
                    admin_password_hash,
                    admin_name,
                    "TurboTIFFLAS",
                    1,
                    "team",
                    "monthly",
                    "active",
                    None,
                    None,
                    now,
                    now,
                ),
            )
        conn.commit()


def get_user_by_id(user_id: int) -> Optional[Dict[str, Any]]:
    with _connect() as conn:
        row = conn.execute("SELECT * FROM users WHERE id = ?", (int(user_id),)).fetchone()
    return _row_to_dict(row)


def get_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    with _connect() as conn:
        row = conn.execute(
            "SELECT * FROM users WHERE lower(email) = lower(?)",
            (str(email).strip(),),
        ).fetchone()
    return _row_to_dict(row)


def create_user(
    email: str,
    password_hash: str,
    full_name: str,
    company: str = "",
    plan_slug: str = "free",
    billing_cycle: str = "monthly",
    subscription_status: str = "trial",
    trial_started_at: Optional[str] = None,
    trial_ends_at: Optional[str] = None,
    is_admin: bool = False,
) -> Dict[str, Any]:
    now = _utc_now()
    normalized_email = str(email).strip().lower()
    normalized_plan = plan_slug if plan_slug in PLAN_CATALOG else "free"
    with _connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO users (
                email, password_hash, full_name, company, is_admin,
                plan_slug, billing_cycle, subscription_status, trial_started_at, trial_ends_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized_email,
                password_hash,
                full_name.strip(),
                company.strip(),
                1 if is_admin else 0,
                normalized_plan,
                billing_cycle,
                subscription_status,
                trial_started_at,
                trial_ends_at,
                now,
                now,
            ),
        )
        user_id = cur.lastrowid
        conn.commit()
    user = get_user_by_id(int(user_id))
    if user is None:
        raise RuntimeError("Failed to create user record.")
    return user


def update_last_login(user_id: int) -> None:
    now = _utc_now()
    with _connect() as conn:
        conn.execute(
            "UPDATE users SET last_login_at = ?, updated_at = ? WHERE id = ?",
            (now, now, int(user_id)),
        )
        conn.commit()


def update_user_subscription(
    user_id: int,
    plan_slug: str,
    billing_cycle: str,
    subscription_status: str,
    trial_started_at: Optional[str] = None,
    trial_ends_at: Optional[str] = None,
) -> None:
    now = _utc_now()
    normalized_plan = plan_slug if plan_slug in PLAN_CATALOG else "free"
    with _connect() as conn:
        conn.execute(
            """
            UPDATE users
            SET plan_slug = ?, billing_cycle = ?, subscription_status = ?, trial_started_at = ?, trial_ends_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (normalized_plan, billing_cycle, subscription_status, trial_started_at, trial_ends_at, now, int(user_id)),
        )
        conn.commit()


def update_user_admin(user_id: int, is_admin: bool) -> None:
    now = _utc_now()
    with _connect() as conn:
        conn.execute(
            "UPDATE users SET is_admin = ?, updated_at = ? WHERE id = ?",
            (1 if is_admin else 0, now, int(user_id)),
        )
        conn.commit()


def create_payment(
    user_id: int,
    plan_slug: str,
    billing_cycle: str,
    amount_cents: int,
    original_amount_cents: Optional[int] = None,
    discount_cents: int = 0,
    coupon_code: Optional[str] = None,
    currency: str = "USD",
    provider: str = "sandbox",
    provider_ref: Optional[str] = None,
    status: str = "pending",
    note: str = "",
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    now = _utc_now()
    normalized_plan = plan_slug if plan_slug in PLAN_CATALOG else "free"
    metadata_json = json.dumps(metadata or {})
    with _connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO payments (
                user_id, plan_slug, billing_cycle, amount_cents, original_amount_cents, discount_cents, coupon_code, currency,
                provider, provider_ref, status, note, metadata_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(user_id),
                normalized_plan,
                billing_cycle,
                int(amount_cents),
                int(original_amount_cents if original_amount_cents is not None else amount_cents),
                max(0, int(discount_cents)),
                (str(coupon_code).strip().upper() if coupon_code else None),
                currency,
                provider,
                provider_ref,
                status,
                note.strip(),
                metadata_json,
                now,
                now,
            ),
        )
        payment_id = cur.lastrowid
        conn.commit()
    payment = get_payment_by_id(int(payment_id))
    if payment is None:
        raise RuntimeError("Failed to create payment record.")
    return payment


def get_payment_by_id(payment_id: int) -> Optional[Dict[str, Any]]:
    with _connect() as conn:
        row = conn.execute("SELECT * FROM payments WHERE id = ?", (int(payment_id),)).fetchone()
    return _row_to_dict(row)


def list_payments(user_id: Optional[int] = None, limit: int = 100) -> List[Dict[str, Any]]:
    query = "SELECT * FROM payments"
    params: List[Any] = []
    if user_id is not None:
        query += " WHERE user_id = ?"
        params.append(int(user_id))
    query += " ORDER BY datetime(created_at) DESC, id DESC LIMIT ?"
    params.append(int(limit))
    with _connect() as conn:
        rows = conn.execute(query, params).fetchall()
    return [_row_to_dict(row) for row in rows if row is not None]


def list_users(limit: int = 200) -> List[Dict[str, Any]]:
    with _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM users ORDER BY datetime(created_at) DESC, id DESC LIMIT ?",
            (int(limit),),
        ).fetchall()
    return [_row_to_dict(row) for row in rows if row is not None]


def _normalize_coupon_code(code: str) -> str:
    return re.sub(r"[^A-Z0-9_-]+", "", str(code or "").upper().strip())


def _normalize_expiry(expires_at: Optional[str]) -> Optional[str]:
    raw = str(expires_at or "").strip()
    if not raw:
        return None
    if len(raw) == 10:
        return f"{raw} 23:59:59 UTC"
    return raw


def create_coupon(
    code: str,
    description: str,
    discount_type: str,
    discount_value: int,
    applies_to_plan: Optional[str] = None,
    active: bool = True,
    max_redemptions: Optional[int] = None,
    expires_at: Optional[str] = None,
) -> Dict[str, Any]:
    normalized_code = _normalize_coupon_code(code)
    if not normalized_code:
        raise ValueError("Coupon code is required.")
    if discount_type not in {"percent", "amount", "trial_days"}:
        raise ValueError("Unsupported coupon type.")
    normalized_plan = str(applies_to_plan or "").strip().lower() or None
    if normalized_plan and normalized_plan not in PLAN_CATALOG:
        raise ValueError("Coupon plan scope is invalid.")
    now = _utc_now()
    with _connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO coupons (
                code, description, discount_type, discount_value, applies_to_plan,
                active, max_redemptions, expires_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized_code,
                str(description or "").strip(),
                discount_type,
                int(discount_value),
                normalized_plan,
                1 if active else 0,
                int(max_redemptions) if max_redemptions not in (None, "", 0, "0") else None,
                _normalize_expiry(expires_at),
                now,
                now,
            ),
        )
        coupon_id = cur.lastrowid
        conn.commit()
    coupon = get_coupon_by_id(int(coupon_id))
    if coupon is None:
        raise RuntimeError("Failed to create coupon.")
    return coupon


def get_coupon_by_id(coupon_id: int) -> Optional[Dict[str, Any]]:
    with _connect() as conn:
        row = conn.execute("SELECT * FROM coupons WHERE id = ?", (int(coupon_id),)).fetchone()
    return _row_to_dict(row)


def get_coupon_by_code(code: str) -> Optional[Dict[str, Any]]:
    normalized_code = _normalize_coupon_code(code)
    if not normalized_code:
        return None
    with _connect() as conn:
        row = conn.execute("SELECT * FROM coupons WHERE code = ?", (normalized_code,)).fetchone()
    return _row_to_dict(row)


def coupon_redemption_count(coupon_id: int) -> int:
    with _connect() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM coupon_redemptions WHERE coupon_id = ?",
            (int(coupon_id),),
        ).fetchone()
    return int(row["c"]) if row else 0


def user_has_coupon_redemption(user_id: int, coupon_id: int) -> bool:
    with _connect() as conn:
        row = conn.execute(
            "SELECT 1 FROM coupon_redemptions WHERE user_id = ? AND coupon_id = ? LIMIT 1",
            (int(user_id), int(coupon_id)),
        ).fetchone()
    return row is not None


def list_coupons(limit: int = 200) -> List[Dict[str, Any]]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT c.*,
                   COALESCE((SELECT COUNT(*) FROM coupon_redemptions cr WHERE cr.coupon_id = c.id), 0) AS redemption_count
            FROM coupons c
            ORDER BY datetime(c.created_at) DESC, c.id DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
    return [_row_to_dict(row) for row in rows if row is not None]


def set_coupon_active(coupon_id: int, active: bool) -> None:
    now = _utc_now()
    with _connect() as conn:
        conn.execute(
            "UPDATE coupons SET active = ?, updated_at = ? WHERE id = ?",
            (1 if active else 0, now, int(coupon_id)),
        )
        conn.commit()


def validate_coupon(
    code: str,
    plan_slug: str,
    user_id: Optional[int] = None,
) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
    coupon = get_coupon_by_code(code)
    if coupon is None:
        return None, "Coupon code was not found."
    if not bool(coupon.get("active")):
        return None, "Coupon code is inactive."
    applies_to_plan = str(coupon.get("applies_to_plan") or "").strip().lower()
    if applies_to_plan and applies_to_plan != str(plan_slug or "").strip().lower():
        return None, f"Coupon only applies to the {applies_to_plan.title()} plan."
    expires_at = str(coupon.get("expires_at") or "").strip()
    if expires_at:
        try:
            expires_dt = datetime.strptime(expires_at, "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=UTC)
            if expires_dt < datetime.now(UTC):
                return None, "Coupon code has expired."
        except Exception:
            pass
    max_redemptions = coupon.get("max_redemptions")
    if max_redemptions not in (None, "") and coupon_redemption_count(int(coupon["id"])) >= int(max_redemptions):
        return None, "Coupon has reached its redemption limit."
    if user_id is not None and user_has_coupon_redemption(int(user_id), int(coupon["id"])):
        return None, "You have already used this coupon."
    return coupon, None


def record_coupon_redemption(
    coupon_id: int,
    user_id: int,
    coupon_code: str,
    discount_type: str,
    discount_value: int,
    amount_cents_applied: int = 0,
    trial_days_applied: int = 0,
    payment_id: Optional[int] = None,
    status: str = "applied",
) -> None:
    now = _utc_now()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO coupon_redemptions (
                coupon_id, user_id, payment_id, coupon_code, discount_type, discount_value,
                amount_cents_applied, trial_days_applied, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(coupon_id),
                int(user_id),
                int(payment_id) if payment_id is not None else None,
                _normalize_coupon_code(coupon_code),
                discount_type,
                int(discount_value),
                int(amount_cents_applied),
                int(trial_days_applied),
                status,
                now,
                now,
            ),
        )
        conn.commit()


def approve_payment(payment_id: int) -> Optional[Dict[str, Any]]:
    payment = get_payment_by_id(int(payment_id))
    if payment is None:
        return None
    now = _utc_now()
    with _connect() as conn:
        conn.execute(
            "UPDATE payments SET status = ?, updated_at = ? WHERE id = ?",
            ("paid", now, int(payment_id)),
        )
        conn.commit()
    update_user_subscription(
        user_id=int(payment["user_id"]),
        plan_slug=str(payment["plan_slug"]),
        billing_cycle=str(payment["billing_cycle"]),
        subscription_status="active",
        trial_started_at=None,
        trial_ends_at=None,
    )
    return get_payment_by_id(int(payment_id))


def admin_summary() -> Dict[str, Any]:
    with _connect() as conn:
        user_count = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
        active_count = conn.execute(
            "SELECT COUNT(*) AS c FROM users WHERE subscription_status = 'active'"
        ).fetchone()["c"]
        payment_count = conn.execute("SELECT COUNT(*) AS c FROM payments").fetchone()["c"]
        coupon_count = conn.execute("SELECT COUNT(*) AS c FROM coupons").fetchone()["c"]
        revenue_cents = conn.execute(
            "SELECT COALESCE(SUM(amount_cents), 0) AS c FROM payments WHERE status = 'paid'"
        ).fetchone()["c"]
    return {
        "users": int(user_count),
        "active_subscriptions": int(active_count),
        "payments": int(payment_count),
        "coupons": int(coupon_count),
        "paid_revenue_cents": int(revenue_cents),
    }


def plan_amount(plan_slug: str, billing_cycle: str) -> int:
    plan = PLAN_CATALOG.get(plan_slug) or PLAN_CATALOG["free"]
    if billing_cycle == "yearly":
        return int(plan["yearly_cents"])
    return int(plan["monthly_cents"])
