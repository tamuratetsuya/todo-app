"""
Blog Backend テストスイート
実行: pytest test_main.py -v
依存: pip install pytest httpx pytest-asyncio
"""

import io
import os
import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_USER", "test")
os.environ.setdefault("DB_PASSWORD", "test")
os.environ.setdefault("DB_NAME", "testdb")
os.environ.setdefault("ADMIN_PASSWORD", "tamura")
os.environ.setdefault("S3_BUCKET", "test-bucket")
os.environ.setdefault("AWS_REGION", "ap-northeast-1")

# DB・S3をモックしてからアプリをインポート
with patch("pymysql.connect"), patch("boto3.client"):
    from main import app

client = TestClient(app)

CORRECT_PASSWORD = "tamura"
WRONG_PASSWORD = "wrong"


# ─────────────────────────────────────────
# ヘルパー
# ─────────────────────────────────────────

def make_cursor(rows=None, one=None):
    """DictCursorのモックを生成"""
    cur = MagicMock()
    cur.__enter__ = lambda s: s
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchall.return_value = rows or []
    cur.fetchone.return_value = one
    cur.lastrowid = 99
    return cur


def make_conn(cursor):
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


def dummy_image():
    return ("test.jpg", io.BytesIO(b"fakeimagecontent"), "image/jpeg")


def dummy_video():
    return ("test.mp4", io.BytesIO(b"fakevideocontent"), "video/mp4")


# ─────────────────────────────────────────
# POST /auth — 認証
# ─────────────────────────────────────────

class TestAuth:
    def test_correct_password(self):
        res = client.post("/auth", data={"password": CORRECT_PASSWORD})
        assert res.status_code == 200
        assert res.json() == {"ok": True}

    def test_wrong_password(self):
        res = client.post("/auth", data={"password": WRONG_PASSWORD})
        assert res.status_code == 401

    def test_missing_password(self):
        res = client.post("/auth", data={})
        assert res.status_code == 422


# ─────────────────────────────────────────
# GET /posts — 投稿一覧
# ─────────────────────────────────────────

class TestListPosts:
    def test_returns_list(self):
        post = {"id": 1, "title": "T", "body": "B", "created_at": "2026-01-01T00:00:00"}
        cur = make_cursor(rows=[post])
        cur.fetchall.side_effect = [[post], []]  # posts, then media for post
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.get("/posts")
        assert res.status_code == 200
        assert isinstance(res.json(), list)

    def test_empty_list(self):
        cur = make_cursor(rows=[])
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.get("/posts")
        assert res.status_code == 200
        assert res.json() == []

    def test_posts_include_media(self):
        post = {"id": 1, "title": "T", "body": "B", "created_at": "2026-01-01T00:00:00"}
        media = [{"id": 10, "post_id": 1, "url": "https://s3.example.com/blog/x.jpg", "media_type": "image"}]
        cur = make_cursor()
        cur.fetchall.side_effect = [[post], media]
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.get("/posts")
        assert res.status_code == 200
        assert res.json()[0]["media"] == media

    def test_ordered_by_created_at_desc(self):
        """クエリにORDER BY created_at DESCが含まれること"""
        cur = make_cursor(rows=[])
        with patch("main.get_conn", return_value=make_conn(cur)):
            client.get("/posts")
        call_args = cur.execute.call_args_list[0][0][0]
        assert "ORDER BY created_at DESC" in call_args


# ─────────────────────────────────────────
# GET /posts/{post_id} — 投稿詳細
# ─────────────────────────────────────────

class TestGetPost:
    def test_found(self):
        post = {"id": 1, "title": "T", "body": "B", "created_at": "2026-01-01T00:00:00"}
        cur = make_cursor(one=post, rows=[])
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.get("/posts/1")
        assert res.status_code == 200
        assert res.json()["id"] == 1

    def test_not_found(self):
        cur = make_cursor(one=None)
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.get("/posts/9999")
        assert res.status_code == 404

    def test_includes_media(self):
        post = {"id": 1, "title": "T", "body": "B", "created_at": "2026-01-01T00:00:00"}
        media = [{"id": 5, "post_id": 1, "url": "https://s3.example.com/blog/a.mp4", "media_type": "video"}]
        cur = make_cursor(one=post, rows=media)
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.get("/posts/1")
        assert res.status_code == 200
        assert res.json()["media"][0]["media_type"] == "video"


# ─────────────────────────────────────────
# POST /posts — 投稿作成
# ─────────────────────────────────────────

class TestCreatePost:
    def test_success_no_files(self):
        post = {"id": 99, "title": "New", "body": "Body", "created_at": "2026-01-01T00:00:00", "media": []}
        cur = make_cursor(one=post, rows=[])
        with patch("main.get_conn", return_value=make_conn(cur)), \
             patch("main.s3") as mock_s3:
            res = client.post("/posts", data={
                "title": "New", "body": "Body", "password": CORRECT_PASSWORD
            })
        assert res.status_code == 201

    def test_wrong_password(self):
        res = client.post("/posts", data={
            "title": "T", "body": "B", "password": WRONG_PASSWORD
        })
        assert res.status_code == 401

    def test_missing_title(self):
        res = client.post("/posts", data={"body": "B", "password": CORRECT_PASSWORD})
        assert res.status_code == 422

    def test_missing_body(self):
        res = client.post("/posts", data={"title": "T", "password": CORRECT_PASSWORD})
        assert res.status_code == 422

    def test_missing_password(self):
        res = client.post("/posts", data={"title": "T", "body": "B"})
        assert res.status_code == 422

    def test_with_image_upload(self):
        post = {"id": 99, "title": "T", "body": "B", "created_at": "2026-01-01T00:00:00", "media": []}
        cur = make_cursor(one=post, rows=[])
        with patch("main.get_conn", return_value=make_conn(cur)), \
             patch("main.s3") as mock_s3:
            name, data, ct = dummy_image()
            res = client.post("/posts",
                data={"title": "T", "body": "B", "password": CORRECT_PASSWORD},
                files=[("files", (name, data, ct))]
            )
        assert res.status_code == 201
        mock_s3.put_object.assert_called_once()

    def test_with_video_upload(self):
        post = {"id": 99, "title": "T", "body": "B", "created_at": "2026-01-01T00:00:00", "media": []}
        cur = make_cursor(one=post, rows=[])
        with patch("main.get_conn", return_value=make_conn(cur)), \
             patch("main.s3") as mock_s3:
            name, data, ct = dummy_video()
            res = client.post("/posts",
                data={"title": "T", "body": "B", "password": CORRECT_PASSWORD},
                files=[("files", (name, data, ct))]
            )
        assert res.status_code == 201
        # media_typeがvideoになること
        insert_call = cur.execute.call_args_list
        media_inserts = [c for c in insert_call if "INSERT INTO media" in str(c)]
        assert any("video" in str(c) for c in media_inserts)

    def test_video_extensions_detected(self):
        """mp4/mov/avi/webmはvideo、それ以外はimageとして判定"""
        post = {"id": 99, "title": "T", "body": "B", "created_at": "2026-01-01T00:00:00", "media": []}
        for ext, expected_type in [("mp4", "video"), ("mov", "video"), ("avi", "video"), ("webm", "video"), ("jpg", "image"), ("png", "image")]:
            cur = make_cursor(one=post, rows=[])
            with patch("main.get_conn", return_value=make_conn(cur)), \
                 patch("main.s3"):
                res = client.post("/posts",
                    data={"title": "T", "body": "B", "password": CORRECT_PASSWORD},
                    files=[("files", (f"file.{ext}", io.BytesIO(b"data"), "application/octet-stream"))]
                )
            assert res.status_code == 201
            insert_calls = [c for c in cur.execute.call_args_list if "INSERT INTO media" in str(c)]
            assert any(expected_type in str(c) for c in insert_calls), f"ext={ext} expected {expected_type}"


# ─────────────────────────────────────────
# PATCH /posts/{post_id} — 投稿編集
# ─────────────────────────────────────────

class TestUpdatePost:
    def test_update_title(self):
        post = {"id": 1, "title": "Updated", "body": "B", "created_at": "2026-01-01T00:00:00"}
        cur = make_cursor(one=post, rows=[])
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.patch(f"/posts/1?password={CORRECT_PASSWORD}",
                json={"title": "Updated"})
        assert res.status_code == 200

    def test_update_body(self):
        post = {"id": 1, "title": "T", "body": "Updated body", "created_at": "2026-01-01T00:00:00"}
        cur = make_cursor(one=post, rows=[])
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.patch(f"/posts/1?password={CORRECT_PASSWORD}",
                json={"body": "Updated body"})
        assert res.status_code == 200

    def test_wrong_password(self):
        res = client.patch(f"/posts/1?password={WRONG_PASSWORD}", json={"title": "T"})
        assert res.status_code == 401

    def test_not_found(self):
        cur = make_cursor(one=None)
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.patch(f"/posts/9999?password={CORRECT_PASSWORD}",
                json={"title": "T"})
        assert res.status_code == 404

    def test_returns_updated_post_with_media(self):
        post = {"id": 1, "title": "Updated", "body": "B", "created_at": "2026-01-01T00:00:00"}
        media = [{"id": 3, "post_id": 1, "url": "https://s3.example.com/x.jpg", "media_type": "image"}]
        cur = make_cursor(one=post, rows=media)
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.patch(f"/posts/1?password={CORRECT_PASSWORD}",
                json={"title": "Updated"})
        assert res.status_code == 200
        assert "media" in res.json()


# ─────────────────────────────────────────
# DELETE /posts/{post_id} — 投稿削除
# ─────────────────────────────────────────

class TestDeletePost:
    def test_success(self):
        cur = make_cursor(rows=[])
        with patch("main.get_conn", return_value=make_conn(cur)), \
             patch("main.s3"):
            res = client.delete(f"/posts/1?password={CORRECT_PASSWORD}")
        assert res.status_code == 204

    def test_wrong_password(self):
        res = client.delete(f"/posts/1?password={WRONG_PASSWORD}")
        assert res.status_code == 401

    def test_deletes_s3_media(self):
        media = [{"url": "https://test-bucket.s3.ap-northeast-1.amazonaws.com/blog/abc.jpg"}]
        cur = make_cursor(rows=media)
        with patch("main.get_conn", return_value=make_conn(cur)), \
             patch("main.s3") as mock_s3:
            res = client.delete(f"/posts/1?password={CORRECT_PASSWORD}")
        assert res.status_code == 204
        mock_s3.delete_object.assert_called_once_with(Bucket="test-bucket", Key="blog/abc.jpg")

    def test_deletes_multiple_media_from_s3(self):
        media = [
            {"url": "https://test-bucket.s3.ap-northeast-1.amazonaws.com/blog/a.jpg"},
            {"url": "https://test-bucket.s3.ap-northeast-1.amazonaws.com/blog/b.mp4"},
        ]
        cur = make_cursor(rows=media)
        with patch("main.get_conn", return_value=make_conn(cur)), \
             patch("main.s3") as mock_s3:
            res = client.delete(f"/posts/1?password={CORRECT_PASSWORD}")
        assert res.status_code == 204
        assert mock_s3.delete_object.call_count == 2


# ─────────────────────────────────────────
# POST /posts/{post_id}/media — メディア追加
# ─────────────────────────────────────────

class TestAddPostMedia:
    def test_success_image(self):
        cur = make_cursor(one={"id": 1})
        with patch("main.get_conn", return_value=make_conn(cur)), \
             patch("main.s3"):
            name, data, ct = dummy_image()
            res = client.post(f"/posts/1/media",
                data={"password": CORRECT_PASSWORD},
                files=[("files", (name, data, ct))]
            )
        assert res.status_code == 201

    def test_success_video(self):
        cur = make_cursor(one={"id": 1})
        with patch("main.get_conn", return_value=make_conn(cur)), \
             patch("main.s3"):
            name, data, ct = dummy_video()
            res = client.post(f"/posts/1/media",
                data={"password": CORRECT_PASSWORD},
                files=[("files", (name, data, ct))]
            )
        assert res.status_code == 201

    def test_wrong_password(self):
        res = client.post(f"/posts/1/media",
            data={"password": WRONG_PASSWORD},
            files=[("files", dummy_image())]
        )
        assert res.status_code == 401

    def test_post_not_found(self):
        cur = make_cursor(one=None)
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.post(f"/posts/9999/media",
                data={"password": CORRECT_PASSWORD},
                files=[("files", dummy_image())]
            )
        assert res.status_code == 404

    def test_uploads_to_s3(self):
        cur = make_cursor(one={"id": 1})
        with patch("main.get_conn", return_value=make_conn(cur)), \
             patch("main.s3") as mock_s3:
            name, data, ct = dummy_image()
            client.post(f"/posts/1/media",
                data={"password": CORRECT_PASSWORD},
                files=[("files", (name, data, ct))]
            )
        mock_s3.put_object.assert_called_once()

    def test_returns_list_of_added_media(self):
        cur = make_cursor(one={"id": 1})
        with patch("main.get_conn", return_value=make_conn(cur)), \
             patch("main.s3"):
            name, data, ct = dummy_image()
            res = client.post(f"/posts/1/media",
                data={"password": CORRECT_PASSWORD},
                files=[("files", (name, data, ct))]
            )
        assert res.status_code == 201
        body = res.json()
        assert isinstance(body, list)
        assert body[0]["media_type"] == "image"
        assert "url" in body[0]


# ─────────────────────────────────────────
# DELETE /media/{media_id} — メディア削除
# ─────────────────────────────────────────

class TestDeleteMedia:
    def test_success(self):
        cur = make_cursor(one={"url": "https://test-bucket.s3.ap-northeast-1.amazonaws.com/blog/x.jpg"})
        with patch("main.get_conn", return_value=make_conn(cur)), \
             patch("main.s3"):
            res = client.delete(f"/media/1?password={CORRECT_PASSWORD}")
        assert res.status_code == 204

    def test_wrong_password(self):
        res = client.delete(f"/media/1?password={WRONG_PASSWORD}")
        assert res.status_code == 401

    def test_not_found(self):
        cur = make_cursor(one=None)
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.delete(f"/media/9999?password={CORRECT_PASSWORD}")
        assert res.status_code == 404

    def test_deletes_from_s3(self):
        cur = make_cursor(one={"url": "https://test-bucket.s3.ap-northeast-1.amazonaws.com/blog/x.jpg"})
        with patch("main.get_conn", return_value=make_conn(cur)), \
             patch("main.s3") as mock_s3:
            client.delete(f"/media/1?password={CORRECT_PASSWORD}")
        mock_s3.delete_object.assert_called_once_with(Bucket="test-bucket", Key="blog/x.jpg")


# ─────────────────────────────────────────
# GET /ogp — OGP取得
# ─────────────────────────────────────────

class TestGetOGP:
    def test_success(self):
        html = """<html><head>
            <meta property="og:title" content="Test Title"/>
            <meta property="og:description" content="Test Desc"/>
            <meta property="og:image" content="https://example.com/img.jpg"/>
            <meta property="og:site_name" content="Example"/>
        </head></html>"""
        mock_res = MagicMock()
        mock_res.text = html
        with patch("main.requests.get", return_value=mock_res):
            res = client.get("/ogp?url=https://example.com")
        assert res.status_code == 200
        data = res.json()
        assert data["title"] == "Test Title"
        assert data["description"] == "Test Desc"
        assert data["image"] == "https://example.com/img.jpg"
        assert data["site_name"] == "Example"

    def test_fallback_to_title_tag(self):
        html = "<html><head><title>Fallback Title</title></head></html>"
        mock_res = MagicMock()
        mock_res.text = html
        with patch("main.requests.get", return_value=mock_res):
            res = client.get("/ogp?url=https://example.com")
        assert res.status_code == 200
        assert res.json()["title"] == "Fallback Title"

    def test_fetch_error_returns_400(self):
        with patch("main.requests.get", side_effect=Exception("timeout")):
            res = client.get("/ogp?url=https://example.com")
        assert res.status_code == 400

    def test_missing_url_param(self):
        res = client.get("/ogp")
        assert res.status_code == 422
