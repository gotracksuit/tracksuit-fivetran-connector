from typing import List, Literal
from dataclasses import dataclass, field
from uuid import uuid4


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
        unique_id_combination = (
            json['accountBrandId'],
            metric['brandId'],
            metric['questionType'],
            metric['category'],
            metric['geography'],
            metric['waveDate']
        )

        funnel_metrics.append(FunnelMetrics(
            id=hash(unique_id_combination),
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


