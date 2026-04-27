from pydantic import BaseModel


class PriceRecord(BaseModel):
    country:        str
    product_type:   str
    price:          float
    currency:       str
    unit:           str
    reporting_date: str
    source:         str


class PricesResponse(BaseModel):
    total:   int
    records: list[PriceRecord]


class PriceMover(BaseModel):
    country:      str
    product_type: str
    previous:     float
    current:      float
    delta:        float
    pct_change:   float


class ComparedDates(BaseModel):
    latest:   str
    previous: str


class MoversResponse(BaseModel):
    top_increases:  list[PriceMover]
    top_decreases:  list[PriceMover]
    compared_dates: ComparedDates | None = None
    note:           str | None = None


class LastRun(BaseModel):
    batch_id:       str | None
    status:         str | None
    started_at:     str | None
    finished_at:    str | None
    total_ingested: int


class QualitySummary(BaseModel):
    total:         int
    valid_count:   int
    invalid_count: int


class PipelineSummaryResponse(BaseModel):
    last_run: LastRun
    quality:  QualitySummary