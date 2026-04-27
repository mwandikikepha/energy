from fastapi import APIRouter, Query
from api.models.schemas import MoversResponse, PipelineSummaryResponse
from app.services.reporting import (
    get_price_movers,
    get_pipeline_summary,
    get_price_history,
    get_all_dates,
)

router = APIRouter()


@router.get("/report/movers", response_model=MoversResponse)
def price_movers(top_n: int = Query(default=5, ge=1, le=20)):
    data = get_price_movers(top_n=top_n)
    return MoversResponse(**data)


@router.get("/report/pipeline", response_model=PipelineSummaryResponse)
def pipeline_summary():
    data = get_pipeline_summary()
    return PipelineSummaryResponse(**data)


@router.get("/report/history")
def price_history(
    country: str = Query(..., description="Country name"),
    product: str = Query(..., description="Product type"),
):
    return {
        "country": country,
        "product": product,
        "history": get_price_history(country, product),
    }


@router.get("/report/dates")
def available_dates():
    return {"dates": get_all_dates()}
