from fastapi import APIRouter, Query
from api.models.schemas import PricesResponse, PriceRecord
from app.services.reporting import get_latest_prices

router = APIRouter()


@router.get("/prices", response_model=PricesResponse)
def latest_prices(
    product: str | None = Query(default=None, description="Filter by product type: gasoline, diesel, lpg, electricity"),
    country: str | None = Query(default=None, description="Filter by country name"),
):
    """
    Returns the latest price per country per product.
    Optionally filter by product type or country name.
    """
    records = get_latest_prices()

    if product:
        records = [r for r in records if r["product_type"] == product.lower()]

    if country:
        records = [r for r in records if r["country"].lower() == country.lower()]

    return PricesResponse(
        total=len(records),
        records=[PriceRecord(**r) for r in records],
    )