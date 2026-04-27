from pymongo import MongoClient
from config.settings import settings

db = MongoClient(settings.mongo_url)[settings.database_name]

dates = db["fuel_prices"].distinct("reporting_date")
dates_sorted = sorted(dates, reverse=True)
print("Distinct reporting_dates in fuel_prices:")
for d in dates_sorted:
    count = db["fuel_prices"].count_documents({"reporting_date": d})
    print(f"  {d}  →  {count} records")

print()
dates_e = db["electricity_prices"].distinct("reporting_date")
print("Distinct reporting_dates in electricity_prices:")
for d in sorted(dates_e, reverse=True):
    count = db["electricity_prices"].count_documents({"reporting_date": d})
    print(f"  {d}  →  {count} records")
