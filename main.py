import requests
import psycopg2
from dateutil.parser import parse
from datetime import datetime, timezone
import time
from dotenv import load_dotenv
import os

now = datetime.now(timezone.utc).isoformat()

# === Jira Config ===
load_dotenv()

# === Jira Config ===
JIRA_URL = os.getenv('JIRA_URL')
JIRA_USERNAME = os.getenv('JIRA_USERNAME')
JIRA_API_TOKEN = os.getenv('JIRA_API_TOKEN')

# === PostgreSQL Config ===
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

print("DB_HOST =", DB_HOST)
print("DB_NAME =", DB_NAME)
print("DB_USER =", DB_USER)
# === DB Connection ===
conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
cursor = conn.cursor()

# === Timestamp tracking ===
def get_last_run_time():
    cursor.execute("SELECT last_run FROM cap_etl_run_status WHERE job_name = 'cap_core_issues_etl'")
    result = cursor.fetchone()
    return result[0] if result else None

def update_last_run_time():
    now = datetime.now(timezone.utc).isoformat()
    cursor.execute("""
        INSERT INTO cap_etl_run_status (job_name, last_run)
        VALUES ('cap_core_issues_etl', %s)
        ON CONFLICT (job_name) DO UPDATE SET last_run = EXCLUDED.last_run;
    """, (now,))
    conn.commit()

# === Fetch Jira Issues ===
def insert_issue_data(issue_data):
    columns = ', '.join(f'"{k}"' for k in issue_data.keys())
    placeholders = ', '.join(['%s'] * len(issue_data))
    values = list(issue_data.values())

    cursor.execute(f"""
        INSERT INTO public.cap_core_issues ({columns})
        VALUES ({placeholders})
        ON CONFLICT (issue_id) DO UPDATE SET
            summary = EXCLUDED.summary,
            status = EXCLUDED.status,
            priority = EXCLUDED.priority,
            product = EXCLUDED.product,
            brand = EXCLUDED.brand,
            components = EXCLUDED.components,
            created = EXCLUDED.created,
            closed = EXCLUDED.closed,
            done_time = EXCLUDED.done_time,
            linked_oi_issue = EXCLUDED.linked_oi_issue,
            issuetype = EXCLUDED.issuetype,
            environment = EXCLUDED.environment;
    """, values)
    conn.commit()

def fetch_issues(jql_query):
    url = f"{JIRA_URL}/rest/api/2/search"
    start_at = 0
    all_issues = []

    while True:
        params = {
            "jql": jql_query,
            "maxResults": 100,
            "startAt": start_at,
            "fields": "summary,status,priority,components,created,customfield_12024,issuelinks,customfield_11997",
            "expand": "changelog"
        }
        response = requests.get(url, auth=(JIRA_USERNAME, JIRA_API_TOKEN), params=params)

        if response.status_code != 200:
            print("❌ Jira fetch error:", response.text)
            break

        issues = response.json().get("issues", [])
        if not issues:
            break

        all_issues.extend(issues)
        start_at += len(issues)

    return all_issues

def process_issue(issue):
    issue_id = issue['key']
    fields = issue['fields']
    changelog = issue.get('changelog', {}).get('histories', [])

    summary = fields.get('summary')
    issuetype = fields.get('issuetype', {}).get('name')
    environment_field = fields.get('customfield_11800')
    environment = environment_field[0].get('value') if isinstance(environment_field, list) and environment_field else None
    status = fields.get('status', {}).get('name')
    priority = fields.get('priority', {}).get('name')
    product_field = fields.get('customfield_12024')
    product = product_field.get('value') if isinstance(product_field, dict) else None
    brand_field = fields.get('customfield_11997')
    brand = brand_field[0].get('value') if isinstance(brand_field, list) and brand_field else None
    components = ', '.join([c['name'] for c in fields.get('components', [])]) if fields.get('components') else None
    created = parse(fields.get('created'))

    linked_oi_issue = None
    for link in fields.get('issuelinks', []):
        linked_issue = link.get('inwardIssue') or link.get('outwardIssue')
        if linked_issue and linked_issue['key'].startswith('OI-'):
            linked_oi_issue = linked_issue['key']
            break

    done_time, closed_time = None, None
    for history in changelog:
        for item in history['items']:
            if item['field'] == 'status':
                if item['toString'] == 'Done':
                    done_time = parse(history['created'])
                elif item['toString'] in ['Closed', 'Released']:
                    closed_time = parse(history['created'])

    time_to_close_days = (closed_time - created).days if closed_time else None

    issue_data = {
        'issue_id': issue_id,
        'summary': summary,
        'status': status,
        'priority': priority,
        'product': product,
        'brand': brand,
        'components': components,
        'created': created.isoformat(),
        'closed': closed_time.isoformat() if closed_time else None,
        'done_time': done_time.isoformat() if done_time else None,
        'linked_oi_issue': linked_oi_issue,
        'time_to_close_days': time_to_close_days,
        'issuetype': issuetype,
        'environment': environment
    }

    insert_issue_data(issue_data)

# === Main ===
def main():
    last_run = get_last_run_time()
    jql = f"project = CAP AND labels = optumcoreplatformissue"
    if last_run:
        jql += f" AND updated >= '{last_run.strftime('%Y-%m-%d %H:%M')}'"
    else:
        jql = f"project = CAP AND labels = optumcoreplatformissue and created >= '2025-02-01'"

    issues = fetch_issues(jql)

    for issue in issues:
        process_issue(issue)
        time.sleep(0.2)

    update_last_run_time()
    cursor.close()
    conn.close()
    print("✅ CAP Issues Updated.")

if __name__ == "__main__":
    main()