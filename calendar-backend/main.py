from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import os
import pymysql
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_conn():
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        cursorclass=pymysql.cursors.DictCursor,
    )


def init_db():
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(200) NOT NULL,
                date DATE NOT NULL,
                start_time VARCHAR(5),
                end_time VARCHAR(5),
                description TEXT,
                user_name VARCHAR(100) NOT NULL,
                color VARCHAR(20) NOT NULL DEFAULT 'blue',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    conn.commit()
    conn.close()


@app.on_event("startup")
def startup():
    init_db()


class EventCreate(BaseModel):
    title: str
    date: str
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    description: Optional[str] = None
    user_name: str
    color: Optional[str] = "blue"


class EventUpdate(BaseModel):
    title: Optional[str] = None
    date: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    description: Optional[str] = None
    user_name: Optional[str] = None
    color: Optional[str] = None


@app.get("/events")
def list_events(year: int, month: int):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM events WHERE YEAR(date) = %s AND MONTH(date) = %s ORDER BY date, start_time",
            (year, month),
        )
        rows = cur.fetchall()
    conn.close()
    for row in rows:
        row["date"] = row["date"].isoformat()
    return rows


@app.post("/events", status_code=201)
def create_event(body: EventCreate):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO events (title, date, start_time, end_time, description, user_name, color) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (body.title, body.date, body.start_time, body.end_time, body.description, body.user_name, body.color),
        )
        conn.commit()
        cur.execute("SELECT * FROM events WHERE id = LAST_INSERT_ID()")
        row = cur.fetchone()
    conn.close()
    row["date"] = row["date"].isoformat()
    return row


@app.patch("/events/{event_id}")
def update_event(event_id: int, body: EventUpdate):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM events WHERE id = %s", (event_id,))
        event = cur.fetchone()
        if not event:
            conn.close()
            raise HTTPException(status_code=404, detail="Not found")
        fields = {k: v for k, v in body.dict().items() if v is not None}
        if fields:
            set_clause = ", ".join(f"{k} = %s" for k in fields)
            cur.execute(
                f"UPDATE events SET {set_clause} WHERE id = %s",
                (*fields.values(), event_id),
            )
            conn.commit()
        cur.execute("SELECT * FROM events WHERE id = %s", (event_id,))
        row = cur.fetchone()
    conn.close()
    row["date"] = row["date"].isoformat()
    return row


@app.delete("/events/{event_id}", status_code=204)
def delete_event(event_id: int):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM events WHERE id = %s", (event_id,))
        conn.commit()
    conn.close()
