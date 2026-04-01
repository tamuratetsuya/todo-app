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
            CREATE TABLE IF NOT EXISTS todos (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                text VARCHAR(500) NOT NULL,
                done BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    conn.commit()
    conn.close()

@app.on_event("startup")
def startup():
    init_db()


class TodoCreate(BaseModel):
    text: str

class TodoUpdate(BaseModel):
    text: Optional[str] = None
    done: Optional[bool] = None


@app.get("/todos")
def list_todos():
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM todos ORDER BY created_at DESC")
        rows = cur.fetchall()
    conn.close()
    return rows


@app.post("/todos", status_code=201)
def create_todo(body: TodoCreate):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("INSERT INTO todos (text) VALUES (%s)", (body.text,))
        conn.commit()
        cur.execute("SELECT * FROM todos WHERE id = LAST_INSERT_ID()")
        row = cur.fetchone()
    conn.close()
    return row


@app.patch("/todos/{todo_id}")
def update_todo(todo_id: int, body: TodoUpdate):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM todos WHERE id = %s", (todo_id,))
        todo = cur.fetchone()
        if not todo:
            conn.close()
            raise HTTPException(status_code=404, detail="Not found")
        if body.text is not None:
            cur.execute("UPDATE todos SET text = %s WHERE id = %s", (body.text, todo_id))
        if body.done is not None:
            cur.execute("UPDATE todos SET done = %s WHERE id = %s", (body.done, todo_id))
        conn.commit()
        cur.execute("SELECT * FROM todos WHERE id = %s", (todo_id,))
        row = cur.fetchone()
    conn.close()
    return row


@app.delete("/todos/{todo_id}", status_code=204)
def delete_todo(todo_id: int):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM todos WHERE id = %s", (todo_id,))
        conn.commit()
    conn.close()
