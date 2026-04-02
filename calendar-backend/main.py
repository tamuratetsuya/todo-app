from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
import os
import uuid
import tempfile
import pymysql
from dotenv import load_dotenv
import requests as http_requests
from bs4 import BeautifulSoup
import boto3
from botocore.exceptions import ClientError

S3_BUCKET = 'golfspace-media'
S3_REGION = 'ap-northeast-1'
CLOUDFRONT_DOMAIN = 'dy9n92jh6ihj.cloudfront.net'
s3_client = boto3.client('s3', region_name=S3_REGION)


def cf_url(key):
    return f"https://{CLOUDFRONT_DOMAIN}/{key}"

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
        cur.execute("""
            CREATE TABLE IF NOT EXISTS participants (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                event_id BIGINT NOT NULL,
                user_name VARCHAR(100) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'join',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_event_user (event_id, user_name),
                FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE
            )
        """)
        # statusカラムがなければ追加
        try:
            cur.execute("""
                ALTER TABLE participants ADD COLUMN status VARCHAR(20) NOT NULL DEFAULT 'join'
            """)
        except Exception:
            pass  # 既にカラムが存在する場合は無視
        cur.execute("""
            CREATE TABLE IF NOT EXISTS event_media (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                event_id BIGINT NOT NULL,
                user_name VARCHAR(100) NOT NULL,
                s3_key VARCHAR(500) NOT NULL,
                file_name VARCHAR(300) NOT NULL,
                media_type VARCHAR(20) NOT NULL,
                comment TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE
            )
        """)
        try:
            cur.execute("ALTER TABLE event_media ADD COLUMN comment TEXT")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE event_media ADD COLUMN thumbnail_key VARCHAR(500)")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE event_media ADD COLUMN category VARCHAR(20) NOT NULL DEFAULT 'photo'")
        except Exception:
            pass
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
        for row in rows:
            row["date"] = row["date"].isoformat()
            cur.execute(
                "SELECT user_name, status FROM participants WHERE event_id = %s ORDER BY created_at",
                (row["id"],),
            )
            rows2 = cur.fetchall()
            row["participants"] = [r["user_name"] for r in rows2 if r["status"] == "join"]
            row["pending"] = [r["user_name"] for r in rows2 if r["status"] == "pending"]
    conn.close()
    return rows


class JoinRequest(BaseModel):
    user_name: str
    status: Optional[str] = "join"


def _get_participants(cur, event_id):
    cur.execute(
        "SELECT user_name, status FROM participants WHERE event_id = %s ORDER BY created_at",
        (event_id,),
    )
    rows = cur.fetchall()
    return {
        "participants": [r["user_name"] for r in rows if r["status"] == "join"],
        "pending": [r["user_name"] for r in rows if r["status"] == "pending"],
    }


@app.post("/events/{event_id}/join", status_code=200)
def join_event(event_id: int, body: JoinRequest):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM events WHERE id = %s", (event_id,))
        if not cur.fetchone():
            conn.close()
            raise HTTPException(status_code=404, detail="Not found")
        status = body.status if body.status in ("join", "pending") else "join"
        try:
            cur.execute(
                "INSERT INTO participants (event_id, user_name, status) VALUES (%s, %s, %s) "
                "ON DUPLICATE KEY UPDATE status = %s",
                (event_id, body.user_name, status, status),
            )
            conn.commit()
        except Exception:
            pass
        result = _get_participants(cur, event_id)
    conn.close()
    return result


@app.delete("/events/{event_id}/join", status_code=200)
def leave_event(event_id: int, body: JoinRequest):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM participants WHERE event_id = %s AND user_name = %s",
            (event_id, body.user_name),
        )
        conn.commit()
        result = _get_participants(cur, event_id)
    conn.close()
    return result


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


@app.get("/og")
def get_og(url: str):
    try:
        res = http_requests.get(url, timeout=6, headers={
            'User-Agent': 'Mozilla/5.0 (compatible; CalendarBot/1.0)'
        })
        res.encoding = res.apparent_encoding
        soup = BeautifulSoup(res.text, 'html.parser')
        def og(prop):
            tag = soup.find('meta', property=f'og:{prop}')
            if not tag:
                tag = soup.find('meta', attrs={'name': prop})
            return tag.get('content') if tag else None
        title = og('title') or (soup.title.string.strip() if soup.title else None)
        image = og('image')
        description = og('description') or soup.find('meta', attrs={'name': 'description'})
        if hasattr(description, 'get'):
            description = description.get('content')
        return {'title': title, 'description': description, 'image': image}
    except Exception:
        raise HTTPException(status_code=400, detail='Failed to fetch OG data')


# ========== メディア ==========

@app.post("/events/{event_id}/media", status_code=201)
async def upload_media(event_id: int, user_name: str = Form(...), file: UploadFile = File(...), comment: str = Form(""), category: str = Form("photo")):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM events WHERE id = %s", (event_id,))
        if not cur.fetchone():
            conn.close()
            raise HTTPException(status_code=404, detail="Not found")

    ext = os.path.splitext(file.filename)[1].lower()
    media_type = 'video' if ext in ('.mp4', '.mov', '.avi', '.webm') else 'image'
    s3_key = f"events/{event_id}/{uuid.uuid4().hex}{ext}"

    content = await file.read()
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=content,
        ContentType=file.content_type or 'application/octet-stream',
    )

    # 動画の場合は最初のフレームをサムネイルとして保存
    thumbnail_key = None
    if media_type == 'video':
        try:
            import cv2
            import numpy as np
            with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
                tmp.write(content)
                tmp_path = tmp.name
            cap = cv2.VideoCapture(tmp_path)
            ret, frame = cap.read()
            cap.release()
            os.unlink(tmp_path)
            if ret and frame is not None:
                _, buf = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
                thumbnail_key = f"events/{event_id}/thumbs/{uuid.uuid4().hex}.jpg"
                s3_client.put_object(
                    Bucket=S3_BUCKET,
                    Key=thumbnail_key,
                    Body=buf.tobytes(),
                    ContentType='image/jpeg',
                )
        except Exception:
            thumbnail_key = None

    with conn.cursor() as cur:
        cat = category if category in ('photo', 'score') else 'photo'
        cur.execute(
            "INSERT INTO event_media (event_id, user_name, s3_key, file_name, media_type, comment, thumbnail_key, category) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (event_id, user_name, s3_key, file.filename, media_type, comment or None, thumbnail_key, cat),
        )
        conn.commit()
        media_id = cur.lastrowid
    conn.close()

    url = cf_url(s3_key)
    thumb_url = cf_url(thumbnail_key) if thumbnail_key else None
    return {'id': media_id, 'url': url, 'media_type': media_type, 'file_name': file.filename, 's3_key': s3_key, 'comment': comment or None, 'thumbnail_url': thumb_url}


@app.get("/events/{event_id}/media")
def list_media(event_id: int):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM event_media WHERE event_id = %s ORDER BY created_at DESC",
            (event_id,),
        )
        rows = cur.fetchall()
    conn.close()
    result = []
    for row in rows:
        row['url'] = cf_url(row['s3_key'])
        row['thumbnail_url'] = cf_url(row['thumbnail_key']) if row.get('thumbnail_key') else None
        row['created_at'] = row['created_at'].isoformat()
        result.append(row)
    return result


@app.delete("/media/{media_id}", status_code=204)
def delete_media(media_id: int):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT s3_key FROM event_media WHERE id = %s", (media_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            raise HTTPException(status_code=404, detail="Not found")
        s3_client.delete_object(Bucket=S3_BUCKET, Key=row['s3_key'])
        cur.execute("DELETE FROM event_media WHERE id = %s", (media_id,))
        conn.commit()
    conn.close()


@app.get("/album")
def get_album():
    """全イベントのメディア数を含む一覧（アルバムページ用）"""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT e.*, COUNT(m.id) as media_count
            FROM events e
            LEFT JOIN event_media m ON e.id = m.event_id
            GROUP BY e.id
            ORDER BY e.date DESC
        """)
        rows = cur.fetchall()
        for row in rows:
            row['date'] = row['date'].isoformat()
            cur.execute(
                "SELECT user_name, status FROM participants WHERE event_id = %s ORDER BY created_at",
                (row['id'],),
            )
            ps = cur.fetchall()
            row['participants'] = [r['user_name'] for r in ps if r['status'] == 'join']
    conn.close()
    return rows
