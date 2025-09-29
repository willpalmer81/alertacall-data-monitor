#!/usr/bin/env python3
"""
Google Chat Alert System for Pipeline Monitoring
Sends formatted cards to Google Chat spaces via webhooks
"""

import requests
import json
from datetime import datetime
from typing import Dict, List, Optional

class GoogleChatAlert:
    """Send rich formatted alerts to Google Chat"""

    def __init__(self, webhook_url: str):
        """
        Initialize with Google Chat webhook URL
        Get from: Chat Space > Apps & integrations > Webhooks > Create
        """
        self.webhook_url = webhook_url

    def send_checkin_card(self, checkin_time: str, status: Dict) -> bool:
        """Send a formatted check-in card to Google Chat"""

        # Determine overall status color and icon
        if status['overall']['health'] == 'CRITICAL':
            header_subtitle = "🔴 CRITICAL ISSUES DETECTED"
            header_image = "https://www.gstatic.com/images/icons/material/system/2x/error_red_48dp.png"
        elif status['overall']['health'] == 'WARNING':
            header_subtitle = "⚠️ WARNINGS PRESENT"
            header_image = "https://www.gstatic.com/images/icons/material/system/2x/warning_amber_48dp.png"
        else:
            header_subtitle = "✅ ALL SYSTEMS OPERATIONAL"
            header_image = "https://www.gstatic.com/images/icons/material/system/2x/check_circle_green_48dp.png"

        # Build the card
        card = {
            "cards": [
                {
                    "header": {
                        "title": f"Pipeline Check-in: {checkin_time}",
                        "subtitle": header_subtitle,
                        "imageUrl": header_image
                    },
                    "sections": [
                        {
                            "header": "Summary",
                            "widgets": [
                                {
                                    "keyValue": {
                                        "topLabel": "Check Time",
                                        "content": status['timestamp']
                                    }
                                },
                                {
                                    "keyValue": {
                                        "topLabel": "Critical Issues",
                                        "content": str(status['overall']['critical'])
                                    }
                                },
                                {
                                    "keyValue": {
                                        "topLabel": "Warnings",
                                        "content": str(status['overall']['warnings'])
                                    }
                                }
                            ]
                        }
                    ]
                }
            ]
        }

        # Add pipeline status section
        pipeline_widgets = []
        for key, pipeline in status['pipelines'].items():
            # Status emoji
            if pipeline['status'] == 'OK':
                emoji = "✅"
            elif pipeline['status'] == 'WARNING':
                emoji = "⚠️"
            elif pipeline['status'] == 'CRITICAL':
                emoji = "🔴"
            else:
                emoji = "⏳"

            # Build detail text
            details = []
            if 'last_record' in pipeline:
                details.append(f"Last: {pipeline['last_record']}")
            if 'hours_stale' in pipeline:
                details.append(f"Age: {pipeline['hours_stale']}h")
            if 'today_count' in pipeline:
                details.append(f"Count: {pipeline['today_count']}")
            if 'exists' in pipeline:
                details.append("File: " + ("EXISTS" if pipeline['exists'] else "MISSING"))

            pipeline_widgets.append({
                "keyValue": {
                    "topLabel": pipeline['name'],
                    "content": f"{emoji} {pipeline['status']}",
                    "bottomLabel": " | ".join(details) if details else "No details"
                }
            })

        card['cards'][0]['sections'].append({
            "header": "Pipeline Details",
            "widgets": pipeline_widgets
        })

        # Add action buttons
        card['cards'][0]['sections'].append({
            "widgets": [
                {
                    "buttons": [
                        {
                            "textButton": {
                                "text": "VIEW DASHBOARD",
                                "onClick": {
                                    "openLink": {
                                        "url": "http://your-vm-ip:5000"
                                    }
                                }
                            }
                        },
                        {
                            "textButton": {
                                "text": "VIEW LOGS",
                                "onClick": {
                                    "openLink": {
                                        "url": "http://your-vm-ip:5000/logs"
                                    }
                                }
                            }
                        }
                    ]
                }
            ]
        })

        # Send to Google Chat
        try:
            response = requests.post(
                self.webhook_url,
                json=card,
                headers={'Content-Type': 'application/json'}
            )
            return response.status_code == 200
        except Exception as e:
            print(f"Failed to send Google Chat alert: {e}")
            return False

    def send_critical_alert(self, pipeline_name: str, issue: str, details: Dict) -> bool:
        """Send immediate critical alert"""

        card = {
            "cards": [
                {
                    "header": {
                        "title": "🚨 CRITICAL PIPELINE FAILURE",
                        "subtitle": pipeline_name,
                        "imageUrl": "https://www.gstatic.com/images/icons/material/system/2x/error_red_48dp.png"
                    },
                    "sections": [
                        {
                            "widgets": [
                                {
                                    "textParagraph": {
                                        "text": f"<b>Issue:</b> {issue}"
                                    }
                                },
                                {
                                    "keyValue": {
                                        "topLabel": "Detection Time",
                                        "content": datetime.now().strftime('%H:%M:%S')
                                    }
                                }
                            ]
                        }
                    ]
                }
            ]
        }

        # Add detail widgets
        if details:
            detail_widgets = []
            for key, value in details.items():
                detail_widgets.append({
                    "keyValue": {
                        "topLabel": key,
                        "content": str(value)
                    }
                })
            card['cards'][0]['sections'].append({
                "header": "Details",
                "widgets": detail_widgets
            })

        # Send
        try:
            response = requests.post(
                self.webhook_url,
                json=card,
                headers={'Content-Type': 'application/json'}
            )
            return response.status_code == 200
        except Exception as e:
            print(f"Failed to send critical alert: {e}")
            return False

    def send_simple_message(self, message: str) -> bool:
        """Send a simple text message"""

        payload = {"text": message}

        try:
            response = requests.post(
                self.webhook_url,
                json=payload,
                headers={'Content-Type': 'application/json'}
            )
            return response.status_code == 200
        except Exception as e:
            print(f"Failed to send message: {e}")
            return False

    def send_daily_summary(self, date: str, summary: Dict) -> bool:
        """Send end-of-day summary card"""

        card = {
            "cards": [
                {
                    "header": {
                        "title": f"📊 Daily Pipeline Summary",
                        "subtitle": date
                    },
                    "sections": [
                        {
                            "widgets": [
                                {
                                    "keyValue": {
                                        "topLabel": "Total Checks",
                                        "content": str(summary.get('total_checks', 0))
                                    }
                                },
                                {
                                    "keyValue": {
                                        "topLabel": "Issues Detected",
                                        "content": str(summary.get('issues_detected', 0))
                                    }
                                },
                                {
                                    "keyValue": {
                                        "topLabel": "Uptime",
                                        "content": f"{summary.get('uptime_percent', 0)}%"
                                    }
                                }
                            ]
                        }
                    ]
                }
            ]
        }

        # Add pipeline-specific stats
        if 'pipeline_stats' in summary:
            stat_widgets = []
            for pipeline, stats in summary['pipeline_stats'].items():
                stat_widgets.append({
                    "keyValue": {
                        "topLabel": pipeline,
                        "content": f"{stats['uptime']}% uptime",
                        "bottomLabel": f"{stats['records_processed']} records"
                    }
                })

            card['cards'][0]['sections'].append({
                "header": "Pipeline Performance",
                "widgets": stat_widgets
            })

        # Send
        try:
            response = requests.post(
                self.webhook_url,
                json=card,
                headers={'Content-Type': 'application/json'}
            )
            return response.status_code == 200
        except Exception as e:
            print(f"Failed to send daily summary: {e}")
            return False


# Example usage and test
if __name__ == "__main__":
    # Test with a sample webhook URL
    # Replace with your actual Google Chat webhook URL
    WEBHOOK_URL = "https://chat.googleapis.com/v1/spaces/XXXXXX/messages?key=YOUR_KEY&token=YOUR_TOKEN"

    chat = GoogleChatAlert(WEBHOOK_URL)

    # Test status for demo
    test_status = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'overall': {
            'health': 'WARNING',
            'critical': 0,
            'warnings': 2
        },
        'pipelines': {
            'etl_calls': {
                'name': 'ETL Call Data',
                'status': 'OK',
                'last_record': '2025-09-29 07:25:00',
                'hours_stale': 1,
                'today_count': 1523
            },
            'csv_morning': {
                'name': 'Morning CSV (11:31)',
                'status': 'WARNING',
                'exists': False
            }
        }
    }

    # Send test check-in
    print("Sending test check-in card...")
    success = chat.send_checkin_card("07:30", test_status)
    print(f"Check-in card sent: {success}")

    # Send test critical alert
    print("Sending test critical alert...")
    success = chat.send_critical_alert(
        "ETL Pipeline",
        "No data received for 24+ hours",
        {"Last Record": "2025-09-28 07:00:00", "Expected Records": "1500+", "Actual": "0"}
    )
    print(f"Critical alert sent: {success}")