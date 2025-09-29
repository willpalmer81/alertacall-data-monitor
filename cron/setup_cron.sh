#!/bin/bash
# Setup cron jobs for pipeline monitoring
# Run this script to install all monitoring cron jobs

MONITOR_DIR="/opt/alertacall-data-monitor"
PYTHON="/usr/bin/python3"

# Color output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Setting up Alertacall Data Monitor cron jobs...${NC}"

# Backup existing crontab
crontab -l > /tmp/crontab_backup_$(date +%Y%m%d_%H%M%S).txt 2>/dev/null
echo "Existing crontab backed up to /tmp/"

# Create the cron entries
cat <<EOF > /tmp/monitor_cron
# Alertacall Data Monitor - Check-in Points
# Morning CC Data Check
30 7 * * * ${PYTHON} ${MONITOR_DIR}/monitors/checkin.py --time morning >> ${MONITOR_DIR}/logs/checkin.log 2>&1

# First Shift Check
45 11 * * * ${PYTHON} ${MONITOR_DIR}/monitors/checkin.py --time first_shift >> ${MONITOR_DIR}/logs/checkin.log 2>&1

# Second Shift Check
45 15 * * * ${PYTHON} ${MONITOR_DIR}/monitors/checkin.py --time second_shift >> ${MONITOR_DIR}/logs/checkin.log 2>&1

# Continuous monitoring (every 5 minutes for critical issues)
*/5 * * * * ${PYTHON} ${MONITOR_DIR}/monitors/pipeline_health.py --quick-check >> ${MONITOR_DIR}/logs/continuous.log 2>&1

# Daily summary report (end of day)
0 17 * * * ${PYTHON} ${MONITOR_DIR}/monitors/daily_summary.py >> ${MONITOR_DIR}/logs/daily.log 2>&1

# Weekly cleanup of old logs (Sunday 2am)
0 2 * * 0 find ${MONITOR_DIR}/logs -name "*.log" -mtime +30 -delete
EOF

echo -e "${YELLOW}The following cron jobs will be added:${NC}"
cat /tmp/monitor_cron

read -p "Do you want to install these cron jobs? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    # Append to existing crontab
    (crontab -l 2>/dev/null; cat /tmp/monitor_cron) | crontab -
    echo -e "${GREEN}Cron jobs installed successfully!${NC}"
    echo "Run 'crontab -l' to verify"
else
    echo "Installation cancelled"
fi

# Create log directory if it doesn't exist
mkdir -p ${MONITOR_DIR}/logs
echo "Log directory created at ${MONITOR_DIR}/logs"

# Cleanup
rm /tmp/monitor_cron