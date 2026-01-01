from flask import Flask, request, jsonify
from flask_mail import Mail, Message
from flask_limiter import Limiter
from flask_cors import CORS
from psycopg2 import connect, OperationalError
from datetime import date, datetime
from dotenv import load_dotenv
from werkzeug.middleware.proxy_fix import ProxyFix
from collections import namedtuple
import os, json, csv, io, hmac, hashlib, requests, logging, time

# ==============================
# ENV + LOGGING
# ==============================
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ==============================
# APP SETUP
# ==============================
app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)

CORS(
    app,
    resources={
        r"/submit": {"origins": r"^https://([a-z0-9-]+\.)?mysqft\.in$"},
        r"/report": {"origins": r"^https://([a-z0-9-]+\.)?mysqft\.in$"},
    }
)

# ==============================
# MAIL
# ==============================
app.config.update(
    MAIL_SERVER=os.getenv("MAIL_SERVER"),
    MAIL_PORT=int(os.getenv("MAIL_PORT", 587)),
    MAIL_USE_TLS=True,
    MAIL_USERNAME=os.getenv("MAIL_USERNAME"),
    MAIL_PASSWORD=os.getenv("MAIL_PASSWORD"),
    MAIL_DEFAULT_SENDER=("MySqft", os.getenv("MAIL_DEFAULT_SENDER")),
)
mail = Mail(app)

# ==============================
# RATE LIMIT
# ==============================
def limiter_key():
    return (
        request.headers.get("CF-Connecting-IP")
        or request.headers.get("X-Forwarded-For", "").split(",")[0]
        or request.remote_addr
    )

limiter = Limiter(app=app, key_func=limiter_key)

# ==============================
# DATABASE
# ==============================
def get_db():
    try:
        conn = connect(
            os.getenv("SUPABASE_DATABASE_URL"),
            sslmode="require",
            connect_timeout=5,
        )
        return conn
    except OperationalError as e:
        logger.error("DB connection failed: %s", e)
        return None

# ==============================
# COMPANY CACHE
# ==============================
Company = namedtuple(
    "Company",
    "id name email discord webhook_url webhook_secret plan expiry fields"
)

COMPANY_CACHE = {}
CACHE_TTL = 300
LAST_LOAD = 0

def load_companies(force=False):
    global LAST_LOAD
    if not force and time.time() - LAST_LOAD < CACHE_TTL:
        return

    conn = get_db()
    if not conn:
        logger.warning("Cannot load companies, DB connection failed")
        return

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT subdomain, id, company_name, email,
                       discord_webhook, webhook_url, webhook_secret,
                       plan, plan_expiry, lead_fields
                FROM companies
                WHERE is_active = true
            """)
            COMPANY_CACHE.clear()
            for row in cur.fetchall():
                COMPANY_CACHE[row[0]] = Company(*row[1:])
        LAST_LOAD = time.time()
        logger.info("Loaded %d active companies", len(COMPANY_CACHE))
    except Exception as e:
        logger.error("Error loading companies: %s", e)
    finally:
        conn.close()

# ==============================
# HELPERS
# ==============================
def resolve_subdomain():
    host = request.headers.get("X-Forwarded-Host", request.host).split(":")[0]
    return host.replace(".mysqft.in", "") if host.endswith(".mysqft.in") else "mysqft"

def days_left(expiry):
    return (expiry - date.today()).days

# ==============================
# DAILY LEADS
# ==============================
def add_daily_lead(company_id):
    conn = get_db()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE companies SET daily_leads = daily_leads + 1 WHERE id = %s",
                (company_id,)
            )
        conn.commit()
        logger.info("Incremented daily leads for company_id=%s", company_id)
    except Exception as e:
        logger.error("Failed to increment daily leads: %s", e)
        conn.rollback()
    finally:
        conn.close()

# ==============================
# SAVE LEAD
# ==============================
def save_lead(company_id, data):
    conn = get_db()
    if not conn:
        return False

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO company_leads (id, company_id, lead_data)
                VALUES (gen_random_uuid(), %s, %s)
                """,
                (company_id, json.dumps(data))
            )
        conn.commit()
        add_daily_lead(company_id)
        logger.info("Lead saved for company_id=%s", company_id)
        return True
    except Exception as e:
        logger.error("save_lead error: %s", e)
        conn.rollback()
        return False
    finally:
        conn.close()

# ==============================
# NOTIFICATIONS
# ==============================
def send_email(to, csv_content, expiry):
    body = "Attached is today's lead report."
    dleft = days_left(expiry)
    if 0 <= dleft < 3:
        body += f"\n\n⚠️ Plan expires in {dleft} day(s)."

    try:
        with app.app_context():
            msg = Message("Daily Lead Report", recipients=[to], body=body)
            msg.attach("leads.csv", "text/csv", csv_content)
            mail.send(msg)
        logger.info("Email sent to %s", to)
    except Exception as e:
        logger.error("Failed to send email to %s: %s", to, e)

def send_discord(webhook, content):
    if not webhook:
        return
    try:
        requests.post(webhook, json={"content": content}, timeout=5)
        logger.info("Discord message sent")
    except Exception as e:
        logger.error("Discord send failed: %s", e)

def send_webhook(url, secret, payload):
    if not url or not secret:
        return
    try:
        body = json.dumps(payload)
        timestamp = str(int(time.time()))
        signature = hmac.new(
            secret.encode(),
            (timestamp + body).encode(),
            hashlib.sha256
        ).hexdigest()

        requests.post(
            url,
            data=body,
            headers={
                "Content-Type": "application/json",
                "X-Signature": signature,
                "X-Timestamp": timestamp,
            },
            timeout=5
        )
        logger.info("Webhook sent to %s", url)
    except Exception as e:
        logger.error("Webhook failed: %s", e)

# ==============================
# DAILY REPORT
# ==============================
def daily_report():
    conn = get_db()
    if not conn:
        logger.error("Cannot run report, DB connection failed")
        return

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, email, plan, plan_expiry
                FROM companies
                WHERE is_active = true AND plan_expiry >= CURRENT_DATE
            """)

            for cid, email, plan, expiry in cur.fetchall():
                cur.execute("""
                    SELECT lead_data, created_at
                    FROM company_leads
                    WHERE company_id=%s AND created_at::date=CURRENT_DATE
                """, (cid,))
                rows = cur.fetchall()
                if not rows:
                    continue

                headers = sorted({k for r, _ in rows for k in r})
                buf = io.StringIO()
                writer = csv.writer(buf)
                writer.writerow(headers + ["created_at"])

                for data, ts in rows:
                    writer.writerow([data.get(h, "") for h in headers] + [ts])

                if plan in ("email", "all"):
                    send_email(email, buf.getvalue(), expiry)

                cur.execute("""
                    UPDATE companies
                    SET total_leads = total_leads + daily_leads,
                        daily_leads = 0
                    WHERE id = %s
                """, (cid,))

                cur.execute("""
                    DELETE FROM company_leads
                    WHERE company_id=%s AND created_at::date=CURRENT_DATE
                """, (cid,))

            conn.commit()
            logger.info("Daily report processed successfully")
    except Exception as e:
        logger.exception("Daily report failed")
        conn.rollback()
    finally:
        conn.close()

# ==============================
# ROUTES
# ==============================
@app.route("/submit", methods=["POST"])
@limiter.limit("5 per 10 minutes")
def submit():
    sub = resolve_subdomain()
    logger.info("Incoming lead from subdomain: %s", sub)

    company = COMPANY_CACHE.get(sub)
    if not company:
        logger.info("Cache miss for subdomain %s, reloading...", sub)
        load_companies(force=True)
        company = COMPANY_CACHE.get(sub)

    if not company:
        logger.warning("No active company found for subdomain %s", sub)
        return jsonify(error="Unauthorized"), 403

    if company.expiry < date.today():
        logger.warning("Company plan expired for subdomain %s", sub)
        return jsonify(error="Unauthorized"), 403

    lead = {f: request.form.get(f) for f in company.fields if request.form.get(f)}
    if not lead:
        logger.warning("No valid lead data submitted for subdomain %s", sub)
        return jsonify(error="No valid data"), 400

    logger.info("Received lead for company %s: %s", company.name, lead)

    # EMAIL / ALL → store
    if company.plan in ("email", "all"):
        save_lead(company.id, lead)

    # DISCORD ONLY → count only
    if company.plan == "discord":
        add_daily_lead(company.id)

    # DISCORD / ALL → notify
    if company.plan in ("discord", "all"):
        msg = f"📩 **New Lead**\n" + "\n".join(f"**{k}**: {v}" for k, v in lead.items())
        send_discord(company.discord, msg)

    # WEBHOOK
    if company.plan in ("webhook", "all"):
        send_webhook(company.webhook_url, company.webhook_secret, {
            "event": "lead.created",
            "lead": lead
        })

    return jsonify(message="Sent successfully"), 200

@app.route("/report")
def report():
    logger.info("Report triggered")
    start_time = time.time()
    try:
        daily_report()
        elapsed = time.time() - start_time
        logger.info("Report completed in %.2f seconds", elapsed)
        return {"status": "ok"}
    except Exception as e:
        logger.exception("Daily report failed")
        return {"error": "failed"}, 500

# ==============================
# RUN
# ==============================
if __name__ == "__main__":
    logger.info("Starting Flask app...")
    load_companies(force=True)
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
