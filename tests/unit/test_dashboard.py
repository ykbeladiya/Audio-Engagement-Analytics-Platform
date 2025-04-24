"""
Unit tests for CloudWatch dashboard setup functionality.
"""

import json
import pytest
from aws.setup_dashboard import create_dashboard, DashboardConfig, DASHBOARD_NAME

def test_create_dashboard_success(cloudwatch_client):
    """Test successful dashboard creation."""
    # Arrange
    expected_dashboard_name = DASHBOARD_NAME

    # Act
    create_dashboard()

    # Assert
    response = cloudwatch_client.get_dashboard(DashboardName=expected_dashboard_name)
    assert response['DashboardName'] == expected_dashboard_name
    assert isinstance(json.loads(response['DashboardBody']), dict)

def test_dashboard_config_widget_creation():
    """Test metric widget configuration creation."""
    # Arrange
    title = "Test Widget"
    metrics = [["TestNamespace", "TestMetric", "TestDimension", "TestValue"]]
    
    # Act
    widget = DashboardConfig.get_metric_widget(title, metrics)
    
    # Assert
    assert widget["type"] == "metric"
    assert widget["properties"]["title"] == title
    assert widget["properties"]["metrics"] == metrics

def test_dashboard_config_full_body():
    """Test complete dashboard body generation."""
    # Act
    dashboard_body = DashboardConfig.get_dashboard_body()
    
    # Assert
    assert "widgets" in dashboard_body
    assert len(dashboard_body["widgets"]) == 3  # We expect 3 widgets
    assert all(w["type"] == "metric" for w in dashboard_body["widgets"])

def test_create_dashboard_updates_existing(cloudwatch_client):
    """Test dashboard update when it already exists."""
    # Arrange
    initial_body = {
        "widgets": [
            {
                "type": "text",
                "width": 24,
                "height": 6,
                "properties": {
                    "markdown": "# Old Dashboard"
                }
            }
        ]
    }
    cloudwatch_client.put_dashboard(
        DashboardName=DASHBOARD_NAME,
        DashboardBody=json.dumps(initial_body)
    )

    # Act
    create_dashboard()

    # Assert
    response = cloudwatch_client.get_dashboard(DashboardName=DASHBOARD_NAME)
    updated_body = json.loads(response['DashboardBody'])
    assert len(updated_body["widgets"]) == 3  # New dashboard should have 3 widgets

def test_create_dashboard_validates_widgets(cloudwatch_client):
    """Test dashboard creation with invalid widget configuration."""
    # Arrange
    invalid_body = {
        "widgets": [
            {
                "type": "invalid_type",
                "width": 24,
                "height": 6
            }
        ]
    }

    # Act & Assert
    with pytest.raises(Exception):
        cloudwatch_client.put_dashboard(
            DashboardName=DASHBOARD_NAME,
            DashboardBody=json.dumps(invalid_body)
        )

def test_create_dashboard_region_configuration():
    """Test dashboard creation with different region configurations."""
    # This test would verify region-specific behavior
    pass  # Implement based on your region handling logic 