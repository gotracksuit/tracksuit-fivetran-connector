import unittest
from data_classes import CategoryInfoApi, GeographyApi, FunnelMetricApi, FunnelMetricsApi, FunnelMetrics

class TestFunnelMetricsApi(unittest.TestCase):
    def test_to_funnel_metrics(self):
        category = CategoryInfoApi(id=1, name="Category1")
        geography = GeographyApi(id=1, name="Geography1")
        metrics = [
            FunnelMetricApi(
                waveDate="2024-06-01",
                brandId=101,
                brandName="Brand1",
                isAccountBrand=True,
                questionType="UNPROMPTED_AWARENESS",
                percentage=50.0,
                population=1000
            )
        ]
        funnel_metrics_api = FunnelMetricsApi(
            accountBrandId=1,
            brandName="Brand1",
            category=category,
            geography=geography,
            metrics=metrics,
            sampleSizeQuality="High"
        )

        result = funnel_metrics_api.to_funnel_metrics()

        expected = [
            FunnelMetrics(
                account_brand_id=1,
                wave_date="2024-06-01",
                brand_id=101,
                brand_name="Brand1",
                is_account_brand="True",
                question_type="UNPROMPTED_AWARENESS",
                filter="",
                percentage=50.0,
                population=1000,
                category_id=1,
                category_name="Category1",
                geography_id=1,
                geography_name="Geography1",
                sample_size_quality="High"
            )
        ]

        self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main()