import uuid
import pandas as pd
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Enum, Time
import logging
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
import pytz
import os
import csv

app = FastAPI()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = "sqlite:///./store_monitoring.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

REPORTS_FOLDER = "reports"
os.makedirs(REPORTS_FOLDER, exist_ok=True)

# DB Models
class StoreStatus(Base):
    __tablename__ = "store_status"
    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(String)
    timestamp_utc = Column(DateTime)
    status = Column(String)  # 'active' or 'inactive'

class BusinessHours(Base):
    __tablename__ = "business_hours"
    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(String)
    day_of_week = Column(Integer)
    start_time_local = Column(Time)
    end_time_local = Column(Time)

class Timezone(Base):
    __tablename__ = "store_timezone"
    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(String)
    timezone_str = Column(String)

class ReportStatus(Base):
    __tablename__ = "report_status"
    id = Column(String, primary_key=True)
    status = Column(String)
    file_path = Column(String, nullable=True)

Base.metadata.create_all(bind=engine)

# Utils

def get_local_business_hours(store_id, date_utc):
    session = SessionLocal()
    tz_record = session.query(Timezone).filter_by(store_id=store_id).first()
    tz = pytz.timezone(tz_record.timezone_str if tz_record else "America/Chicago")
    local_date = date_utc.astimezone(tz)
    dow = local_date.weekday()
    business_hour = session.query(BusinessHours).filter_by(store_id=store_id, day_of_week=dow).first()
    session.close()
    if not business_hour:
        return datetime.combine(local_date.date(), datetime.min.time()).replace(tzinfo=tz), datetime.combine(local_date.date(), datetime.max.time()).replace(tzinfo=tz)
    start = datetime.combine(local_date.date(), business_hour.start_time_local).replace(tzinfo=tz)
    end = datetime.combine(local_date.date(), business_hour.end_time_local).replace(tzinfo=tz)
    return start, end


def generate_report(report_id: str):
    session = SessionLocal()
    try:
        latest_timestamp = session.query(StoreStatus.timestamp_utc).order_by(StoreStatus.timestamp_utc.desc()).first()[0]
        now = latest_timestamp
        one_hour = now - timedelta(hours=1)
        one_day = now - timedelta(days=1)
        one_week = now - timedelta(weeks=1)

        stores = session.query(StoreStatus.store_id).distinct()

        report_data = []
        for store in stores:
            store_id = store.store_id
            stats = {"store_id": store_id}
            for label, start_time in [("last_hour", one_hour), ("last_day", one_day), ("last_week", one_week)]:
                records = session.query(StoreStatus).filter(
                    StoreStatus.store_id == store_id,
                    StoreStatus.timestamp_utc >= start_time,
                    StoreStatus.timestamp_utc <= now
                ).order_by(StoreStatus.timestamp_utc).all()
                uptime = 0
                downtime = 0

                for i in range(len(records) - 1):
                    current = records[i]
                    nxt = records[i+1]
                    delta = (nxt.timestamp_utc - current.timestamp_utc).total_seconds() / 60
                    current_time_aware = current.timestamp_utc.replace(tzinfo=pytz.UTC)
                    business_start, business_end = get_local_business_hours(store_id, current.timestamp_utc)
                    if business_start <= current_time_aware <= business_end:
                        if current.status == "active":
                            uptime += delta
                        else:
                            downtime += delta

                stats[f"uptime_{label}"] = round(uptime / 60, 2) if "day" in label else round(uptime, 2)
                stats[f"downtime_{label}"] = round(downtime / 60, 2) if "day" in label else round(downtime, 2)

            report_data.append(stats)
            file_path = os.path.join(REPORTS_FOLDER, f"{report_id}.csv")

            if report_data:
                with open(file_path, mode='w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=report_data[0].keys())
                    writer.writeheader()
                    writer.writerows(report_data)
                report = session.query(ReportStatus).filter_by(id=report_id).first()
                report.status = "Complete"
                report.file_path = file_path
            else:
                logger.info(f"No data available for report {report_id}")
                report.status = "Complete"
                report.file_path = None
            session.commit()
    except Exception as e:
        report = session.query(ReportStatus).filter_by(id=report_id).first()
        report.status = f"Failed: {str(e)}"
        session.commit()
    finally:
        session.close()



# API Endpoints

@app.post("/trigger_report")
def trigger_report(background_tasks: BackgroundTasks):
    report_id = str(uuid.uuid4())
    session = SessionLocal()
    report = ReportStatus(id=report_id, status="Running")
    session.add(report)
    session.commit()
    session.close()
    background_tasks.add_task(generate_report, report_id)
    return {"report_id": report_id}


@app.get("/get_report")
def get_report(report_id: str):
    session = SessionLocal()
    report = session.query(ReportStatus).filter_by(id=report_id).first()
    session.close()

    if not report:
        raise HTTPException(status_code=404, detail="Report ID not found")

    if report.status == "Running":
        return {"status": "Running"}

    if report.status.startswith("Failed"):
        return {"status": report.status}

    if report.file_path and os.path.exists(report.file_path):
        return {"status": "Complete", "file_path": report.file_path}

    return {"status": "Complete", "message": "No file was generated."}
