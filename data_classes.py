from typing import List, Literal
from dataclasses import dataclass

@dataclass
class CategoryInfoApi:
    id: int
    name: str

@dataclass
class GeographyApi:
    id: int
    name: str

@dataclass
class FunnelMetricApi:
    waveDate: str
    brandId: int
    brandName: str
    isAccountBrand: bool
    questionType: Literal[
        "UNPROMPTED_AWARENESS",
        "PROMPTED_AWARENESS",
        "INVESTIGATION",
        "CONSIDERATION",
        "PREFERENCE",
        "USAGE"
    ]
    percentage: float
    population: int

@dataclass
class FunnelMetricsApi:
    accountBrandId: int
    brandName: str
    category: CategoryInfoApi
    geography: GeographyApi
    metrics: List[FunnelMetricApi]
    sampleSizeQuality: str

    def to_funnel_metrics(self):
        return [
            FunnelMetrics(
                account_brand_id=self.accountBrandId,
                wave_date=metric.waveDate,
                brand_id=metric.brandId,
                brand_name=metric.brandName,
                is_account_brand=str(metric.isAccountBrand),
                question_type=metric.questionType,
                percentage=metric.percentage,
                population=metric.population,
                category_id=self.category.id,
                category_name=self.category.name,
                geography_id=self.geography.id,
                geography_name=self.geography.name,
                sample_size_quality=self.sampleSizeQuality
            )
            for metric in self.metrics
        ]

@dataclass
class FunnelMetrics:
    account_brand_id: int
    wave_date: str
    brand_id: int
    brand_name: str
    is_account_brand: str
    question_type: str
    percentage: float
    population: int
    account_brand_id: int
    category_id: int
    category_name: str
    geography_id: int
    geography_name: str
    sample_size_quality: str
