# Alertacall Data Monitor

**Real-time monitoring and observability for Alertacall's data pipelines**

## Overview

This monitoring system provides automated health checks at three critical check-in points throughout the day, with both web dashboard visibility and Google Chat/email alerts.

## Check-in Points

- **07:30** - Morning CC Data Check
  - ETL overnight run verification
  - Data freshness check
  - Operator count validation

- **11:45** - First Shift Check
  - Morning CSV file (11:31) presence
  - Productivity upload verification
  - Call volume monitoring

- **15:45** - Second Shift Check
  - Afternoon CSV file (15:35) presence
  - Productivity upload verification
  - Inbound sheet updates

## Components

### 1. Web Dashboard (`dashboard/pipeline_dashboard.py`)
- Real-time health status at http://your-vm:5000
- Auto-refreshes every 60 seconds
- Color-coded status indicators (OK/WARNING/CRITICAL)
- JSON API endpoint for integration

### 2. Google Chat Integration (`alerts/google_chat_alerts.py`)
- Rich card formatting with status colors
- Check-in summaries with pipeline details
- Critical alerts for immediate issues
- Daily summary reports
- Action buttons linking to dashboard

### 3. Email Alerts (`alerts/email_alerts.py`)
- HTML formatted check-in emails
- Sent at each check-in point
- Summary of all pipeline statuses
- Links to live dashboard

### 4. Health Monitors (`monitors/`)
- `pipeline_health.py` - Core ETL pipeline monitoring
- `csv_watcher.py` - Productivity CSV file monitoring
- `data_freshness.py` - Data staleness detection

## Setup

### 1. Google Chat Webhook

1. Open your Google Chat space
2. Click space name > Apps & integrations
3. Click "Add webhooks"
4. Name it "Pipeline Monitor"
5. Copy the webhook URL

### 2. Configuration

Create `config/monitoring_config.yaml`:

```yaml
database:
  host: localhost
  database: contact_centre_analytics
  user: etl_user
  password: ${MYSQL_PASSWORD}

google_chat:
  webhook_url: "https://chat.googleapis.com/v1/spaces/XXX/messages?key=XXX&token=XXX"
  enabled: true

email:
  smtp_server: smtp.gmail.com
  smtp_port: 587
  sender: willpalmer@alertacall.com
  password: ${EMAIL_PASSWORD}
  recipients:
    - willpalmer@alertacall.com

monitoring:
  csv_directory: /home/will/daily_reports
  log_directory: /var/log/pipeline_monitor

thresholds:
  data_stale_hours: 24
  warning_hours: 12
  min_daily_calls: 1000
  expected_operators: 237
```

### 3. Cron Jobs

Add to crontab:

```bash
# Morning CC Data Check
30 7 * * * /opt/alertacall-data-monitor/cron/checkin_730.sh

# First Shift Check
45 11 * * * /opt/alertacall-data-monitor/cron/checkin_1145.sh

# Second Shift Check
45 15 * * * /opt/alertacall-data-monitor/cron/checkin_1545.sh

# Continuous monitoring (every 5 minutes)
*/5 * * * * /opt/alertacall-data-monitor/monitors/pipeline_health.py --quick-check
```

### 4. Install Dependencies

```bash
pip install -r requirements.txt
```

Requirements:
- flask
- mysql-connector-python
- requests
- pyyaml
- python-dateutil

## Dashboard Access

Once running, access the dashboard at:
- **Local**: http://localhost:5000
- **Network**: http://[your-vm-ip]:5000
- **API**: http://[your-vm-ip]:5000/api/status

## Google Chat Messages

The system sends different card types to Google Chat:

1. **Check-in Cards** (07:30, 11:45, 15:45)
   - Color-coded header (green/amber/red)
   - Pipeline status summary
   - Individual pipeline details
   - Action buttons to dashboard

2. **Critical Alerts** (immediate)
   - Red alert icon
   - Specific failure details
   - Timestamp of detection
   - Required actions

3. **Daily Summary** (end of day)
   - Overall uptime percentage
   - Issues detected count
   - Per-pipeline statistics

## Alert Logic

### Status Levels

- **OK** ✅ - All checks passing
- **WARNING** ⚠️ - Non-critical issues (e.g., data 12-24 hours old)
- **CRITICAL** 🔴 - Urgent issues (e.g., data >24 hours old, missing files)
- **PENDING** ⏳ - Waiting for scheduled event

### Escalation

1. First detection: Google Chat card + dashboard update
2. If critical: Additional email to willpalmer@alertacall.com
3. If persists >1 hour: Follow-up alert with escalation note

## Monitored Pipelines

| Pipeline | Table/File | Check Type | Critical Threshold |
|----------|------------|------------|-------------------|
| ETL Calls | FactCalls | Freshness | >24 hours |
| First Calls | FactFirstCalls | Record count | 0 records today |
| Operators | DimOperators | Count | <200 operators |
| Morning CSV | 11:31 file | File exists | Missing after 12:00 |
| Afternoon CSV | 15:35 file | File exists | Missing after 16:00 |
| Productivity Upload | Google Sheets | Timestamp | >2 hours old |

## Troubleshooting

### No Google Chat Messages
1. Check webhook URL in config
2. Test with: `python alerts/google_chat_alerts.py`
3. Check firewall/proxy settings

### Dashboard Not Accessible
1. Ensure Flask is running: `ps aux | grep pipeline_dashboard`
2. Check port 5000 is open: `netstat -an | grep 5000`
3. Try localhost first, then external IP

### Data Shows as Stale
1. Check ETL scheduler: `crontab -l`
2. Verify database connectivity
3. Check `/var/log/etl_scripts/` for errors

## Future Enhancements

- [ ] Historical trend graphs
- [ ] Predictive failure alerts
- [ ] Mobile-responsive dashboard
- [ ] REST API for pipeline control
- [ ] Integration with ticketing system
- [ ] Automated recovery actions