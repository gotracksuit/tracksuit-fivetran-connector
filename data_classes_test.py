import pytest
from data_classes import json_to_funnel_metrics

sample_json = {
    "accountBrandId": 123,
    "category": {
        "id": 1,
        "name": "Beverages"
    },
    "geography": {
        "id": 10,
        "name": "USA"
    },
    "metrics": [
        {
            "accountBrandId": 123,
            "brandId": 456,
            "brandName": "Coke",
            "filter": "All",
            "filterType": "None",
            "waveDate": "2023-08-01",
            "questionType": "Preference",
            "category": "Beverages",
            "geography": "USA",
            "base": 100,
            "weight": 1.5,
            "baseWeight": 1.0,
            "percentage": 50.0
        }
    ]
}


def test_json_to_funnel_metrics():
    result = json_to_funnel_metrics(sample_json)

    metric = result[0]
    assert (metric.account_brand_id == 123)
    assert (metric.brand_id == 456)
    assert (metric.brand_name == "Coke")
    assert (metric.filter == "All")
    assert (metric.filter_type == "None")
    assert (metric.wave_date == "2023-08-01")
    assert (metric.question_type == "Preference")
    assert (metric.category_name == "Beverages")
    assert (metric.geography_name == "USA")
    assert (metric.base == 100)
    assert (metric.weight == 1.5)
    assert (metric.base_weight == 1.0)
    assert (metric.percentage == 50.0)
