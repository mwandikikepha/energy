from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from api.routers import prices, reports

app = FastAPI(
    title="Global Energy Price Platform",
    description="Real-time global fuel and electricity prices across 150+ countries.",
    version="1.0.0",
)

app.mount("/static", StaticFiles(directory="api/static"), name="static")
templates = Jinja2Templates(directory="api/templates")

app.include_router(prices.router,  prefix="/api")
app.include_router(reports.router, prefix="/api")


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    return templates.TemplateResponse(request, "dashboard.html")