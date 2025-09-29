#!/usr/bin/env python3
"""
Pipeline Health Monitor for Alertacall ETL
Monitors all critical data pipelines and alerts on issues
Location: /opt/etl_scripts/pipeline_health_monitor.py
Schedule: Run every hour via ETL scheduler
"""

import mysql.connector
import json
import logging
from datetime import datetime, timedelta
import os
import sys
import requests

# Configuration
DB_CONFIG = {
    'host': os.environ.get('MYSQL_HOST', 'localhost'),
    'database': 'contact_centre_analytics',
    'user': os.environ.get('MYSQL_USER', 'etl_user'),
    'password': os.environ.get('MYSQL_PASSWORD')
}

# Slack webhook for alerts (optional)
SLACK_WEBHOOK = os.environ.get('SLACK_WEBHOOK_URL')

# Email settings (using existing ETL email config)
ALERT_EMAIL = 'willpalmer@alertacall.com'

# Logging setup (matches ETL scripts pattern)
LOG_DIR = '/var/log/etl_scripts'
LOG_FILE = os.path.join(LOG_DIR, f'pipeline_health_{datetime.now().strftime("%Y%m%d")}.log')

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
        'warning_hours': 12,
        'min_daily_records': 1000,
        'check_after_hour': 12  # Only check volume after noon
    },
    'FactFirstCalls': {
        'table': 'FactFirstCalls', 
        'date_column': 'alert_date',
        'critical_hours': 24,
        'warning_hours': 12,
        'min_daily_records': 500,
        'check_after_hour': 12
    },
    'DimAgentActivity': {
        'table': 'DimAgentActivity',
        'date_column': 'date',
        'critical_hours': 48,
        'warning_hours': 24,
        'min_daily_records': 0,  # Can be 0 on weekends
        'check_after_hour': 99   # Don't check volume
    },
    'messaging_result': {
        'table': 'dl_pellonia_messaging_result',
        'date_column': 'createdAt',
        'critical_hours': 48,
        'warning_hours': 24,
        'min_daily_records': 0,  # Varies greatly
        'check_after_hour': 99,
        'extra_where': 'AND dl_is_current = true'
    },
    'pellonia_tickets': {
        'table': 'dl_pellonia_tickets',
        'date_column': 'dateCreated',
        'critical_hours': 48,
        'warning_hours': 24,
        'min_daily_records': 0,
        'check_after_hour': 99,
        'extra_where': 'AND dl_is_current = true'
    }
}

def check_pipeline_health(cursor, pipeline_name, config):
    """Check health of a single pipeline"""
    
    extra_where = config.get('extra_where', '')
    
    # Query for latest record and today's count
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
            'last_record': None
        }
    
    hours_stale = result['hours_stale']
    records_today = result['records_today']
    
    # Determine health status
    if hours_stale > config['critical_hours']:
        status = 'CRITICAL'
    elif hours_stale > config['warning_hours']:
        status = 'WARNING'
    else:
        status = 'OK'
    
    # Check volume (only after specified hour)
    volume_status = 'NORMAL'
    current_hour = datetime.now().hour
    
    if (current_hour >= config['check_after_hour'] and 
        records_today < config['min_daily_records']):
        volume_status = 'LOW_VOLUME'
        if status == 'OK':
            status = 'WARNING'
    
    return {
        'pipeline': pipeline_name,
        'status': status,
        'hours_stale': hours_stale,
        'records_today': records_today,
        'last_record': result['last_record'],
        'volume_status': volume_status,
        'message': f"{hours_stale}h since update, {records_today} records today"
    }

def send_alerts(issues):
    """Send alerts via configured channels"""
    
    if not issues:
        return
    
    # Format alert message
    alert_lines = [f"🚨 Pipeline Health Alert - {datetime.now().strftime('%Y-%m-%d %H:%M')}"]
    
    critical_count = sum(1 for i in issues if i['status'] == 'CRITICAL')
    warning_count = sum(1 for i in issues if i['status'] == 'WARNING')
    
    if critical_count > 0:
        alert_lines.append(f"❌ {critical_count} CRITICAL issue(s)")
    if warning_count > 0:
        alert_lines.append(f"⚠️ {warning_count} WARNING issue(s)")
    
    alert_lines.append("")
    
    for issue in issues:
        emoji = "❌" if issue['status'] == 'CRITICAL' else "⚠️"
        alert_lines.append(f"{emoji} {issue['pipeline']}: {issue['message']}")
    
    alert_text = "\n".join(alert_lines)
    
    # Log to file
    logger.warning(alert_text)
    
    # Send to Slack if configured
    if SLACK_WEBHOOK:
        try:
            response = requests.post(SLACK_WEBHOOK, json={'text': alert_text})
            if response.status_code == 200:
                logger.info("Alert sent to Slack")
        except Exception as e:
            logger.error(f"Failed to send Slack alert: {e}")
    
    # Write to alert file for email script to pick up
    alert_file = os.path.join(LOG_DIR, 'pipeline_alerts.txt')
    with open(alert_file, 'a') as f:
        f.write(f"\n{datetime.now().isoformat()}\n")
        f.write(alert_text)
        f.write("\n" + "="*50 + "\n")

def create_health_summary(all_results):
    """Create summary table for logging"""
    
    summary_lines = []
    summary_lines.append("\n" + "="*80)
    summary_lines.append("PIPELINE HEALTH SUMMARY")
    summary_lines.append("="*80)
    summary_lines.append(f"{'Pipeline':<20} {'Status':<10} {'Hours':<8} {'Today':<10} {'Volume':<12}")
    summary_lines.append("-"*80)
    
    for result in all_results:
        summary_lines.append(
            f"{result['pipeline']:<20} "
            f"{result['status']:<10} "
            f"{result['hours_stale']:<8} "
            f"{result['records_today']:<10} "
            f"{result.get('volume_status', 'N/A'):<12}"
        )
    
    summary_lines.append("="*80)
    
    return "\n".join(summary_lines)

def main():
    """Main monitoring function"""
    
    logger.info("Starting pipeline health check")
    
    try:
        # Connect to database
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        # Check each pipeline
        all_results = []
        issues = []
        
        for pipeline_name, config in PIPELINE_CONFIGS.items():
            logger.info(f"Checking {pipeline_name}...")
            result = check_pipeline_health(cursor, pipeline_name, config)
            all_results.append(result)
            
            if result['status'] in ['CRITICAL', 'WARNING']:
                issues.append(result)
        
        # Log summary
        summary = create_health_summary(all_results)
        logger.info(summary)
        
        # Send alerts if issues found
        if issues:
            send_alerts(issues)
            logger.warning(f"Found {len(issues)} pipeline issue(s)")
            
            # Exit with error code for scheduler to detect
            sys.exit(1)
        else:
            logger.info("✅ All pipelines healthy")
            
            # Create success marker file
            success_file = os.path.join(LOG_DIR, 'pipeline_health_last_success.txt')
            with open(success_file, 'w') as f:
                f.write(datetime.now().isoformat())
        
        cursor.close()
        conn.close()
        
    except mysql.connector.Error as e:
        logger.error(f"Database error: {e}")
        sys.exit(2)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(3)

if __name__ == "__main__":
    main()