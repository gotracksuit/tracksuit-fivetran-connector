from typing import List
from dataclasses import dataclass
from uuid import uuid4
from hashlib import sha256
from json import dumps


@dataclass
class FunnelMetrics:
    id: uuid4
    account_brand_id: int
    brand_id: int
    brand_name: str
    filter: str
    filter_type: str
    wave_date: str
    category_name: str
    geography_name: str
    base: int
    weight: float
    base_weight: float
    percentage: float
    question_type: str


def json_to_funnel_metrics(json) -> List[FunnelMetrics]:
    funnel_metrics = []
    for metric in json['metrics']:
        id = sha256(dumps(metric, sort_keys=True).encode()).hexdigest()
        funnel_metrics.append(FunnelMetrics(
            id=id,
            account_brand_id=json['accountBrandId'],
            brand_id=metric['brandId'],
            brand_name=metric['brandName'],
            filter=metric['filter'],
            filter_type=metric['filterType'],
            wave_date=metric['waveDate'],
            category_name=metric['category'],
            geography_name=metric['geography'],
            base=metric['base'],
            weight=metric['weight'],
            base_weight=metric['baseWeight'],
            percentage=metric['percentage'],
            question_type=metric['questionType']
        ))

    return funnel_metrics


def mock_funnel_metric(
        id="1",
        account_brand_id=2,
        brand_id=1,
        brand_name="brand_name",
        filter="filter",
        filter_type="filter_type",
        wave_date="10/10/2020",
        category_name="category_name",
        geography_name="geography_name",
        base=1,
        weight=1.0,
        base_weight=1.0,
        percentage=1.0,
        question_type="question_type"
):
    return FunnelMetrics(
        id=id,
        account_brand_id=account_brand_id,
        brand_id=brand_id,
        brand_name=brand_name,
        filter=filter,
        filter_type=filter_type,
        wave_date=wave_date,
        category_name=category_name,
        geography_name=geography_name,
        base=base,
        weight=weight,
        base_weight=base_weight,
        percentage=percentage,
        question_type=question_type
    )
