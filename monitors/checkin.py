#!/usr/bin/env python3
"""
Pipeline Check-in Monitor for Alertacall ETL
Sends scheduled check-in reports to Google Chat at key business moments
Location: /opt/alertacall-data-monitor/monitors/checkin.py
Schedule: 07:30 (morning), 11:45 (first_shift), 15:45 (second_shift)
"""

import mysql.connector
import json
import logging
import argparse
from datetime import datetime, timedelta
import os
import sys
import requests
import yaml

# Load configuration
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', 'config', 'monitoring_config.yaml')

try:
    with open(CONFIG_PATH, 'r') as f:
        config = yaml.safe_load(f)
except Exception as e:
    print(f"Error loading config: {e}")
    sys.exit(1)

# Database configuration
DB_CONFIG = {
    'host': config['database']['host'],
    'database': config['database']['database'],
    'user': config['database']['user'],
    'password': config['database']['password']
}

# Google Chat webhook
GOOGLE_CHAT_WEBHOOK = config['alerts']['google_chat']['webhook_url']

# Logging setup
LOG_DIR = '/opt/alertacall-data-monitor/logs'
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f'checkin_{datetime.now().strftime("%Y%m%d")}.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Check-in configurations
CHECKIN_CONFIGS = {
    'morning': {
        'title': '☀️ Morning CC Data Check',
        'time': '07:30',
        'description': 'ETL overnight run verification',
        'critical_pipelines': ['FactCalls', 'FactFirstCalls', 'DimAgentActivity'],
        'checks': {
            'data_freshness': 24,  # hours
            'operator_count': 237  # expected operators
        }
    },
    'first_shift': {
        'title': '📊 First Shift Check',
        'time': '11:45',
        'description': 'Morning CSV and productivity verification',
        'critical_pipelines': ['FactCalls', 'DimAgentActivity'],
        'checks': {
            'csv_file_time': '11:31',
            'productivity_upload': True,
            'call_volume': True
        }
    },
    'second_shift': {
        'title': '📈 Second Shift Check',
        'time': '15:45',
        'description': 'Afternoon CSV and productivity verification',
        'critical_pipelines': ['FactCalls', 'DimAgentActivity'],
        'checks': {
            'csv_file_time': '15:35',
            'productivity_upload': True,
            'inbound_sheet_update': True
        }
    }
}

# Pipeline configurations
PIPELINE_CONFIGS = {
    'FactCalls': {
        'table': 'FactCalls',
        'date_column': 'calldate',
        'critical_hours': 24,
        'min_daily_records': 1000
    },
    'FactFirstCalls': {
        'table': 'FactFirstCalls',
        'date_column': 'alert_date',
        'critical_hours': 24,
        'min_daily_records': 500
    },
    'DimAgentActivity': {
        'table': 'DimAgentActivity',
        'date_column': 'date',
        'critical_hours': 48,
        'min_daily_records': 0
    },
    'messaging_result': {
        'table': 'dl_pellonia_messaging_result',
        'date_column': 'createdAt',
        'critical_hours': 48,
        'extra_where': 'AND dl_is_current = true'
    },
    'pellonia_tickets': {
        'table': 'dl_pellonia_tickets',
        'date_column': 'dateCreated',
        'critical_hours': 48,
        'extra_where': 'AND dl_is_current = true'
    }
}

def check_pipeline_health(cursor, pipeline_name, config):
    """Check health of a single pipeline"""

    extra_where = config.get('extra_where', '')

    query = f"""
    SELECT
        MAX({config['date_column']}) as last_record,
        COUNT(CASE WHEN DATE({config['date_column']}) = CURDATE() THEN 1 END) as records_today,
        TIMESTAMPDIFF(HOUR, MAX({config['date_column']}), NOW()) as hours_stale
    FROM {config['table']}
    WHERE {config['date_column']} >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
    {extra_where}
    """

    cursor.execute(query)
    result = cursor.fetchone()

    if not result or result['last_record'] is None:
        return {
            'pipeline': pipeline_name,
            'status': 'CRITICAL',
            'message': 'No data found in last 7 days',
            'hours_stale': 999,
            'records_today': 0,
            'emoji': '❌'
        }

    hours_stale = result['hours_stale']
    records_today = result['records_today']

    # Determine health status
    if hours_stale > config['critical_hours']:
        status = 'CRITICAL'
        emoji = '❌'
    elif hours_stale > (config['critical_hours'] / 2):
        status = 'WARNING'
        emoji = '⚠️'
    else:
        status = 'OK'
        emoji = '✅'

    return {
        'pipeline': pipeline_name,
        'status': status,
        'hours_stale': hours_stale,
        'records_today': records_today,
        'emoji': emoji,
        'message': f"{hours_stale}h old, {records_today} records today"
    }

def check_operator_count(cursor):
    """Check current operator count in DimOperator"""
    try:
        query = """
        SELECT COUNT(DISTINCT operator_id) as operator_count
        FROM DimOperator
        WHERE is_active = 1
        """
        cursor.execute(query)
        result = cursor.fetchone()
        return result['operator_count'] if result else 0
    except Exception as e:
        logger.error(f"Error checking operator count: {e}")
        return 0

def create_google_chat_card(checkin_type, checkin_config, pipeline_results, operator_count=None):
    """Create a Google Chat card message"""

    # Count issues
    critical_count = sum(1 for r in pipeline_results if r['status'] == 'CRITICAL')
    warning_count = sum(1 for r in pipeline_results if r['status'] == 'WARNING')
    ok_count = sum(1 for r in pipeline_results if r['status'] == 'OK')

    # Overall status
    if critical_count > 0:
        overall_status = '🔴 CRITICAL'
        header_color = '#EA4335'
    elif warning_count > 0:
        overall_status = '🟡 WARNING'
        header_color = '#FBBC04'
    else:
        overall_status = '🟢 ALL OK'
        header_color = '#34A853'

    # Build pipeline status text
    pipeline_text = ""
    for result in pipeline_results:
        pipeline_text += f"{result['emoji']} *{result['pipeline']}*: {result['message']}\n"

    # Add operator count for morning check
    operator_text = ""
    if operator_count is not None:
        if operator_count == 237:
            operator_text = f"\n✅ *Operators*: {operator_count}/237 (expected)"
        elif operator_count >= 230:
            operator_text = f"\n⚠️ *Operators*: {operator_count}/237 (slightly off)"
        else:
            operator_text = f"\n❌ *Operators*: {operator_count}/237 (CRITICAL)"

    # Create card
    card = {
        "cards": [{
            "header": {
                "title": checkin_config['title'],
                "subtitle": f"{checkin_config['description']} | {overall_status}",
                "imageUrl": "https://fonts.gstatic.com/s/i/productlogos/admin_2020q4/v6/192px.svg"
            },
            "sections": [{
                "widgets": [{
                    "textParagraph": {
                        "text": f"<b>Check Time:</b> {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
                               f"<b>Pipeline Status:</b>\n{pipeline_text}"
                               f"{operator_text}\n\n"
                               f"<b>Summary:</b> {ok_count} OK | {warning_count} Warning | {critical_count} Critical"
                    }
                }]
            }]
        }]
    }

    return card

def send_google_chat_alert(card):
    """Send alert to Google Chat"""
    try:
        response = requests.post(
            GOOGLE_CHAT_WEBHOOK,
            headers={'Content-Type': 'application/json; charset=UTF-8'},
            data=json.dumps(card)
        )

        if response.status_code == 200:
            logger.info("✅ Alert sent to Google Chat")
            return True
        else:
            logger.error(f"Failed to send to Google Chat: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        logger.error(f"Error sending to Google Chat: {e}")
        return False

def main():
    """Main check-in function"""

    parser = argparse.ArgumentParser(description='Pipeline check-in monitor')
    parser.add_argument('--time', required=True, choices=['morning', 'first_shift', 'second_shift'],
                       help='Check-in time period')
    args = parser.parse_args()

    checkin_type = args.time
    checkin_config = CHECKIN_CONFIGS[checkin_type]

    logger.info(f"Starting {checkin_config['title']} at {datetime.now()}")

    try:
        # Connect to database
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        # Check critical pipelines for this check-in time
        pipeline_results = []
        for pipeline_name in checkin_config['critical_pipelines']:
            if pipeline_name in PIPELINE_CONFIGS:
                logger.info(f"Checking {pipeline_name}...")
                result = check_pipeline_health(cursor, pipeline_name, PIPELINE_CONFIGS[pipeline_name])
                pipeline_results.append(result)

        # Check operator count for morning check-in
        operator_count = None
        if checkin_type == 'morning':
            operator_count = check_operator_count(cursor)
            logger.info(f"Operator count: {operator_count}")

        # Create and send Google Chat card
        card = create_google_chat_card(checkin_type, checkin_config, pipeline_results, operator_count)
        send_google_chat_alert(card)

        # Log summary
        critical_count = sum(1 for r in pipeline_results if r['status'] == 'CRITICAL')
        warning_count = sum(1 for r in pipeline_results if r['status'] == 'WARNING')

        if critical_count > 0:
            logger.warning(f"Check-in complete: {critical_count} CRITICAL, {warning_count} WARNING")
            sys.exit(1)
        elif warning_count > 0:
            logger.warning(f"Check-in complete: {warning_count} WARNING")
            sys.exit(0)
        else:
            logger.info("✅ Check-in complete: All pipelines healthy")
            sys.exit(0)

        cursor.close()
        conn.close()

    except mysql.connector.Error as e:
        logger.error(f"Database error: {e}")
        sys.exit(2)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(3)

if __name__ == "__main__":
    main()