import unittest
from data_classes import json_to_funnel_metrics, FunnelMetrics

class TestFunnelMetricsTransforms(unittest.TestCase):
    def setUp(self):
        self.sample_json = {
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


    def test_json_to_funnel_metrics(self):
        result = json_to_funnel_metrics(self.sample_json)

        metric = result[0]
        self.assertEqual(metric.account_brand_id, 123)
        self.assertEqual(metric.brand_id, 456)
        self.assertEqual(metric.brand_name, "Coke")
        self.assertEqual(metric.filter, "All")
        self.assertEqual(metric.filter_type, "None")
        self.assertEqual(metric.wave_date, "2023-08-01")
        self.assertEqual(metric.question_type, "Preference")
        self.assertEqual(metric.category_name, "Beverages")
        self.assertEqual(metric.geography_name, "USA")
        self.assertEqual(metric.base, 100)
        self.assertEqual(metric.weight, 1.5)
        self.assertEqual(metric.base_weight, 1.0)
        self.assertEqual(metric.percentage, 50.0)

if __name__ == '__main__':
    unittest.main()
