from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
import os
import pymysql
import boto3
import uuid
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")
S3_BUCKET = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")

s3 = boto3.client("s3", region_name=AWS_REGION)


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
            CREATE TABLE IF NOT EXISTS posts (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(500) NOT NULL,
                body TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS media (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                post_id BIGINT NOT NULL,
                url VARCHAR(1000) NOT NULL,
                media_type VARCHAR(10) NOT NULL,
                FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE
            )
        """)
    conn.commit()
    conn.close()


@app.on_event("startup")
def startup():
    init_db()


# OGP取得
@app.get("/ogp")
def get_ogp(url: str):
    try:
        res = requests.get(url, timeout=5, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(res.text, "html.parser")
        def og(prop):
            tag = soup.find("meta", property=f"og:{prop}") or soup.find("meta", attrs={"name": f"og:{prop}"})
            return tag["content"] if tag and tag.get("content") else None
        title = og("title") or (soup.title.string.strip() if soup.title else None)
        return {
            "url": url,
            "title": title,
            "description": og("description") or "",
            "image": og("image") or "",
            "site_name": og("site_name") or "",
        }
    except Exception:
        raise HTTPException(status_code=400, detail="OGP取得失敗")


# 認証
@app.post("/auth")
def auth(password: str = Form(...)):
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="パスワードが違います")
    return {"ok": True}


# 投稿一覧
@app.get("/posts")
def list_posts():
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM posts ORDER BY created_at DESC")
        posts = cur.fetchall()
        for post in posts:
            cur.execute("SELECT * FROM media WHERE post_id = %s", (post["id"],))
            post["media"] = cur.fetchall()
    conn.close()
    return posts


# 投稿詳細
@app.get("/posts/{post_id}")
def get_post(post_id: int):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM posts WHERE id = %s", (post_id,))
        post = cur.fetchone()
        if not post:
            conn.close()
            raise HTTPException(status_code=404, detail="Not found")
        cur.execute("SELECT * FROM media WHERE post_id = %s", (post_id,))
        post["media"] = cur.fetchall()
    conn.close()
    return post


class PostUpdate(BaseModel):
    title: Optional[str] = None
    body: Optional[str] = None


# 投稿編集
@app.patch("/posts/{post_id}")
def update_post(post_id: int, password: str, update: PostUpdate):
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="パスワードが違います")
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM posts WHERE id = %s", (post_id,))
        if not cur.fetchone():
            conn.close()
            raise HTTPException(status_code=404, detail="Not found")
        if update.title is not None:
            cur.execute("UPDATE posts SET title = %s WHERE id = %s", (update.title, post_id))
        if update.body is not None:
            cur.execute("UPDATE posts SET body = %s WHERE id = %s", (update.body, post_id))
        conn.commit()
        cur.execute("SELECT * FROM posts WHERE id = %s", (post_id,))
        post = cur.fetchone()
        cur.execute("SELECT * FROM media WHERE post_id = %s", (post_id,))
        post["media"] = cur.fetchall()
    conn.close()
    return post


# 投稿作成
@app.post("/posts", status_code=201)
async def create_post(
    title: str = Form(...),
    body: str = Form(...),
    password: str = Form(...),
    files: List[UploadFile] = File(default=[]),
):
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="パスワードが違います")

    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("INSERT INTO posts (title, body) VALUES (%s, %s)", (title, body))
        conn.commit()
        post_id = cur.lastrowid

        for file in files:
            if not file.filename:
                continue
            ext = file.filename.rsplit(".", 1)[-1].lower()
            key = f"blog/{uuid.uuid4()}.{ext}"
            content = await file.read()
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=content,
                ContentType=file.content_type,
            )
            url = f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{key}"
            media_type = "video" if ext in ("mp4", "mov", "avi", "webm") else "image"
            cur.execute(
                "INSERT INTO media (post_id, url, media_type) VALUES (%s, %s, %s)",
                (post_id, url, media_type),
            )
        conn.commit()

        cur.execute("SELECT * FROM posts WHERE id = %s", (post_id,))
        post = cur.fetchone()
        cur.execute("SELECT * FROM media WHERE post_id = %s", (post_id,))
        post["media"] = cur.fetchall()

    conn.close()
    return post


# メディア追加
@app.post("/posts/{post_id}/media", status_code=201)
async def add_post_media(
    post_id: int,
    password: str,
    files: List[UploadFile] = File(default=[]),
):
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="パスワードが違います")
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM posts WHERE id = %s", (post_id,))
        if not cur.fetchone():
            conn.close()
            raise HTTPException(status_code=404, detail="Not found")
        added = []
        for file in files:
            if not file.filename:
                continue
            ext = file.filename.rsplit(".", 1)[-1].lower()
            key = f"blog/{uuid.uuid4()}.{ext}"
            content = await file.read()
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=content,
                ContentType=file.content_type,
            )
            url = f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{key}"
            media_type = "video" if ext in ("mp4", "mov", "avi", "webm") else "image"
            cur.execute(
                "INSERT INTO media (post_id, url, media_type) VALUES (%s, %s, %s)",
                (post_id, url, media_type),
            )
            added.append({"url": url, "media_type": media_type})
        conn.commit()
    conn.close()
    return added


# メディア削除
@app.delete("/media/{media_id}", status_code=204)
def delete_media(media_id: int, password: str):
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="パスワードが違います")
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT url FROM media WHERE id = %s", (media_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            raise HTTPException(status_code=404, detail="Not found")
        key = row["url"].split(".amazonaws.com/")[-1]
        try:
            s3.delete_object(Bucket=S3_BUCKET, Key=key)
        except Exception:
            pass
        cur.execute("DELETE FROM media WHERE id = %s", (media_id,))
        conn.commit()
    conn.close()


# 投稿削除
@app.delete("/posts/{post_id}", status_code=204)
def delete_post(post_id: int, password: str):
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="パスワードが違います")
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT url FROM media WHERE post_id = %s", (post_id,))
        media_list = cur.fetchall()
        for m in media_list:
            key = m["url"].split(".amazonaws.com/")[-1]
            try:
                s3.delete_object(Bucket=S3_BUCKET, Key=key)
            except Exception:
                pass
        cur.execute("DELETE FROM posts WHERE id = %s", (post_id,))
        conn.commit()
    conn.close()
