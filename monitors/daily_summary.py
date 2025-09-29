#!/usr/bin/env python3
"""
Daily Summary Report for Alertacall ETL
Sends end-of-day summary to Google Chat with full pipeline status
Location: /opt/alertacall-data-monitor/monitors/daily_summary.py
Schedule: 17:00 daily
"""

import mysql.connector
import json
import logging
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
GOOGLE_CHAT_WEBHOOK = config['google_chat']['webhook_url']

# Logging setup
LOG_DIR = '/opt/alertacall-data-monitor/logs'
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f'daily_summary_{datetime.now().strftime("%Y%m%d")}.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Pipeline configurations
PIPELINE_CONFIGS = {
    'FactCalls': {
        'table': 'FactCalls',
        'date_column': 'calldate',
        'critical_hours': 24,
        'description': 'Call records'
    },
    'FactFirstCalls': {
        'table': 'FactFirstCalls',
        'date_column': 'alert_date',
        'critical_hours': 24,
        'description': 'First call alerts'
    },
    'DimAgentActivity': {
        'table': 'DimAgentActivity',
        'date_column': 'date',
        'critical_hours': 48,
        'description': 'Agent activity'
    },
    'messaging_result': {
        'table': 'dl_pellonia_messaging_result',
        'date_column': 'createdAt',
        'critical_hours': 48,
        'description': 'Pellonia messages',
        'extra_where': 'AND dl_is_current = true'
    },
    'pellonia_tickets': {
        'table': 'dl_pellonia_tickets',
        'date_column': 'dateCreated',
        'critical_hours': 48,
        'description': 'Pellonia tickets',
        'extra_where': 'AND dl_is_current = true'
    }
}

def check_pipeline_health(cursor, pipeline_name, config):
    """Check health of a single pipeline with detailed stats"""

    extra_where = config.get('extra_where', '')

    query = f"""
    SELECT
        MAX({config['date_column']}) as last_record,
        COUNT(CASE WHEN DATE({config['date_column']}) = CURDATE() THEN 1 END) as records_today,
        COUNT(CASE WHEN DATE({config['date_column']}) = CURDATE() - INTERVAL 1 DAY THEN 1 END) as records_yesterday,
        TIMESTAMPDIFF(HOUR, MAX({config['date_column']}), NOW()) as hours_stale,
        COUNT(*) as records_last_7_days
    FROM {config['table']}
    WHERE {config['date_column']} >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
    {extra_where}
    """

    cursor.execute(query)
    result = cursor.fetchone()

    if not result or result['last_record'] is None:
        return {
            'pipeline': pipeline_name,
            'description': config['description'],
            'status': 'CRITICAL',
            'emoji': '❌',
            'hours_stale': 999,
            'records_today': 0,
            'records_yesterday': 0,
            'trend': '📉',
            'last_record': None
        }

    hours_stale = result['hours_stale']
    records_today = result['records_today']
    records_yesterday = result['records_yesterday']

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

    # Determine trend
    if records_yesterday == 0:
        trend = '➖'
    elif records_today > records_yesterday:
        trend = '📈'
    elif records_today < records_yesterday:
        trend = '📉'
    else:
        trend = '➖'

    return {
        'pipeline': pipeline_name,
        'description': config['description'],
        'status': status,
        'emoji': emoji,
        'hours_stale': hours_stale,
        'records_today': records_today,
        'records_yesterday': records_yesterday,
        'records_last_7_days': result['records_last_7_days'],
        'trend': trend,
        'last_record': result['last_record'].strftime('%Y-%m-%d %H:%M') if result['last_record'] else 'N/A'
    }

def get_daily_stats(cursor):
    """Get additional daily statistics"""
    stats = {}

    try:
        # Total calls today
        cursor.execute("""
            SELECT COUNT(*) as call_count
            FROM FactCalls
            WHERE DATE(calldate) = CURDATE()
        """)
        result = cursor.fetchone()
        stats['calls_today'] = result['call_count'] if result else 0

        # Active operators
        cursor.execute("""
            SELECT COUNT(DISTINCT operator_id) as operator_count
            FROM DimOperator
            WHERE is_active = 1
        """)
        result = cursor.fetchone()
        stats['active_operators'] = result['operator_count'] if result else 0

        # First calls today
        cursor.execute("""
            SELECT COUNT(*) as first_call_count
            FROM FactFirstCalls
            WHERE DATE(alert_date) = CURDATE()
        """)
        result = cursor.fetchone()
        stats['first_calls_today'] = result['first_call_count'] if result else 0

    except Exception as e:
        logger.error(f"Error getting daily stats: {e}")

    return stats

def create_google_chat_card(pipeline_results, daily_stats):
    """Create a Google Chat card for daily summary"""

    # Count statuses
    critical_count = sum(1 for r in pipeline_results if r['status'] == 'CRITICAL')
    warning_count = sum(1 for r in pipeline_results if r['status'] == 'WARNING')
    ok_count = sum(1 for r in pipeline_results if r['status'] == 'OK')

    # Overall status
    if critical_count > 0:
        overall_status = '🔴 ISSUES DETECTED'
        header_color = '#EA4335'
    elif warning_count > 0:
        overall_status = '🟡 WARNINGS'
        header_color = '#FBBC04'
    else:
        overall_status = '🟢 ALL SYSTEMS HEALTHY'
        header_color = '#34A853'

    # Build pipeline summary text
    pipeline_text = "*Pipeline Status:*\n"
    for result in pipeline_results:
        pipeline_text += (f"{result['emoji']} *{result['pipeline']}* ({result['description']})\n"
                         f"   Last update: {result['hours_stale']}h ago | "
                         f"Today: {result['records_today']} {result['trend']}\n")

    # Build daily stats text
    stats_text = "\n*Daily Statistics:*\n"
    if daily_stats:
        stats_text += f"📞 Total calls today: {daily_stats.get('calls_today', 'N/A'):,}\n"
        stats_text += f"👥 Active operators: {daily_stats.get('active_operators', 'N/A')}\n"
        stats_text += f"🚨 First calls today: {daily_stats.get('first_calls_today', 'N/A')}\n"

    summary_text = f"\n*Summary:* {ok_count} OK | {warning_count} Warning | {critical_count} Critical"

    # Create card
    card = {
        "cards": [{
            "header": {
                "title": "📊 Daily Pipeline Summary",
                "subtitle": f"{datetime.now().strftime('%A, %B %d, %Y')} | {overall_status}",
                "imageUrl": "https://fonts.gstatic.com/s/i/productlogos/admin_2020q4/v6/192px.svg"
            },
            "sections": [{
                "widgets": [{
                    "textParagraph": {
                        "text": f"<b>Report Time:</b> {datetime.now().strftime('%H:%M')}\n\n"
                               f"{pipeline_text}"
                               f"{stats_text}"
                               f"{summary_text}"
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
            logger.info("✅ Daily summary sent to Google Chat")
            return True
        else:
            logger.error(f"Failed to send to Google Chat: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        logger.error(f"Error sending to Google Chat: {e}")
        return False

def main():
    """Main daily summary function"""

    logger.info(f"Starting daily summary at {datetime.now()}")

    try:
        # Connect to database
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        # Check all pipelines
        pipeline_results = []
        for pipeline_name, pipeline_config in PIPELINE_CONFIGS.items():
            logger.info(f"Checking {pipeline_name}...")
            result = check_pipeline_health(cursor, pipeline_name, pipeline_config)
            pipeline_results.append(result)

        # Get daily statistics
        daily_stats = get_daily_stats(cursor)

        # Create and send Google Chat card
        card = create_google_chat_card(pipeline_results, daily_stats)
        send_google_chat_alert(card)

        # Log summary
        critical_count = sum(1 for r in pipeline_results if r['status'] == 'CRITICAL')
        warning_count = sum(1 for r in pipeline_results if r['status'] == 'WARNING')

        logger.info(f"Daily summary complete: {critical_count} CRITICAL, {warning_count} WARNING")

        # Log detailed results
        logger.info("\nDetailed Results:")
        for result in pipeline_results:
            logger.info(f"  {result['emoji']} {result['pipeline']}: "
                       f"{result['hours_stale']}h old, "
                       f"{result['records_today']} today, "
                       f"{result['records_yesterday']} yesterday")

        cursor.close()
        conn.close()

        sys.exit(0)

    except mysql.connector.Error as e:
        logger.error(f"Database error: {e}")
        sys.exit(2)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(3)

if __name__ == "__main__":
    main()