#!/usr/bin/env python3
"""
Script to create a CloudWatch dashboard for audiobook event metrics.
Follows AWS best practices for error handling, logging, and configuration management.
"""

import json
import logging
import os
from typing import Dict, Any
import boto3
from botocore.exceptions import ClientError, BotoCoreError

# Configure logging with structured format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
DASHBOARD_NAME = os.getenv('DASHBOARD_NAME', 'AudiobookEventsMetrics')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')

class DashboardConfig:
    """Configuration class for CloudWatch dashboard."""
    
    @staticmethod
    def get_metric_widget(title: str, metrics: list, width: int = 12, height: int = 6,
                         stat: str = 'Sum', period: int = 300) -> Dict[str, Any]:
        """
        Create a metric widget configuration.
        
        Args:
            title: Widget title
            metrics: List of metrics to display
            width: Widget width (default: 12)
            height: Widget height (default: 6)
            stat: Statistic to display (default: Sum)
            period: Period in seconds (default: 300)
            
        Returns:
            Dict containing widget configuration
        """
        return {
            "type": "metric",
            "width": width,
            "height": height,
            "properties": {
                "metrics": metrics,
                "view": "timeSeries",
                "stacked": False,
                "region": AWS_REGION,
                "title": title,
                "period": period,
                "stat": stat
            }
        }

    @classmethod
    def get_dashboard_body(cls) -> Dict[str, Any]:
        """
        Get the complete dashboard configuration.
        
        Returns:
            Dict containing the complete dashboard configuration
        """
        return {
            "widgets": [
                # Events Processing Overview
                cls.get_metric_widget(
                    "Event Processing Overview",
                    [
                        ["AudiobookEvents", "EventsProcessedPerMinute", "Function", "ProcessAudiobookEvents"],
                        [".", "ProcessedEvents", ".", "."],
                        [".", "FailedEvents", ".", "."]
                    ]
                ),
                # Latency Metrics
                cls.get_metric_widget(
                    "Processing Latency",
                    [
                        ["AudiobookEvents", "S3WriteLatency", "Bucket", "audio-engagement-data"],
                        [".", "DynamoDBWriteLatency", "Table", "PlaybackEvents"],
                        [".", "TotalProcessingTime", "Function", "ProcessAudiobookEvents"]
                    ],
                    stat="Average"
                ),
                # Event Success Rate
                cls.get_metric_widget(
                    "Event Processing Success Rate",
                    [
                        [{
                            "expression": "(m1 / (m1 + m2)) * 100",
                            "label": "Success Rate (%)",
                            "id": "e1"
                        }],
                        ["AudiobookEvents", "ProcessedEvents", "Function", "ProcessAudiobookEvents", {"id": "m1"}],
                        [".", "FailedEvents", ".", ".", {"id": "m2"}]
                    ]
                )
            ]
        }

class DashboardManager:
    """Manager class for CloudWatch dashboard operations."""
    
    def __init__(self):
        """Initialize the DashboardManager with AWS client."""
        self.cloudwatch = boto3.client('cloudwatch', region_name=AWS_REGION)
        self.config = DashboardConfig()
    
    def create_or_update_dashboard(self) -> None:
        """Create or update the CloudWatch dashboard."""
        try:
            dashboard_body = self.config.get_dashboard_body()
            self.cloudwatch.put_dashboard(
                DashboardName=DASHBOARD_NAME,
                DashboardBody=json.dumps(dashboard_body)
            )
            logger.info(f"Successfully created/updated dashboard: {DASHBOARD_NAME}")
            
            # Get and log the dashboard URL
            dashboard_url = (
                f"https://{AWS_REGION}.console.aws.amazon.com/cloudwatch/home"
                f"?region={AWS_REGION}#dashboards:name={DASHBOARD_NAME}"
            )
            logger.info(f"Dashboard URL: {dashboard_url}")
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            logger.error(f"AWS Error ({error_code}): {error_message}")
            raise
        except BotoCoreError as e:
            logger.error(f"Boto3 Error: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            raise

def create_dashboard() -> None:
    """Create the CloudWatch dashboard."""
    dashboard_manager = DashboardManager()
    dashboard_manager.create_or_update_dashboard()

def main() -> None:
    """Main function."""
    try:
        create_dashboard()
    except Exception as e:
        logger.error(f"Failed to create dashboard: {str(e)}")
        raise SystemExit(1)

if __name__ == '__main__':
    main() 