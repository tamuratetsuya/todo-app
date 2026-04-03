"""
カレンダーAPI テストケース
全仕様・全機能を網羅
"""
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock, AsyncMock
import json
import io

# DBとS3をモックしてmain.pyをインポート
mock_conn = MagicMock()
mock_s3 = MagicMock()

with patch("pymysql.connect", return_value=mock_conn), \
     patch("boto3.client", return_value=mock_s3):
    from main import app

client = TestClient(app)


# ============================================================
# ヘルパー
# ============================================================

def make_cursor(rows=None, fetchone_val=None):
    cur = MagicMock()
    cur.__enter__ = lambda s: cur
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchall.return_value = rows or []
    cur.fetchone.return_value = fetchone_val
    cur.lastrowid = 1
    return cur


def make_conn(cursor):
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


# ============================================================
# 1. イベント一覧取得  GET /events
# ============================================================

class TestListEvents:

    def test_正常_指定年月のイベントを返す(self):
        cur = make_cursor(rows=[
            {"id": 1, "title": "○○ゴルフ場", "date": MagicMock(isoformat=lambda: "2026-04-10"),
             "start_time": "09:00", "end_time": "13:00", "description": None,
             "user_name": "tamura", "color": "blue", "created_at": "2026-04-01T00:00:00"}
        ])
        cur.fetchall.side_effect = [
            [{"id": 1, "title": "○○ゴルフ場", "date": MagicMock(isoformat=lambda: "2026-04-10"),
              "start_time": "09:00", "end_time": "13:00", "description": None,
              "user_name": "tamura", "color": "blue", "created_at": "2026-04-01T00:00:00"}],
            [{"user_name": "tamura", "status": "join"}, {"user_name": "yamada", "status": "pending"}],
        ]
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.get("/events?year=2026&month=4")
        assert res.status_code == 200
        data = res.json()
        assert len(data) == 1
        assert data[0]["title"] == "○○ゴルフ場"
        assert data[0]["participants"] == ["tamura"]
        assert data[0]["pending"] == ["yamada"]

    def test_正常_イベントなしの月は空配列(self):
        cur = make_cursor(rows=[], fetchone_val=None)
        cur.fetchall.side_effect = [[], ]
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.get("/events?year=2026&month=1")
        assert res.status_code == 200
        assert res.json() == []

    def test_異常_yearパラメータなし(self):
        res = client.get("/events?month=4")
        assert res.status_code == 422

    def test_異常_monthパラメータなし(self):
        res = client.get("/events?year=2026")
        assert res.status_code == 422

    def test_参加者ステータス分離_joinとpendingが正しく分かれる(self):
        cur = MagicMock()
        cur.__enter__ = lambda s: cur
        cur.__exit__ = MagicMock(return_value=False)
        cur.fetchall.side_effect = [
            [{"id": 1, "title": "test", "date": MagicMock(isoformat=lambda: "2026-04-01"),
              "start_time": None, "end_time": None, "description": None,
              "user_name": "a", "color": "green", "created_at": "2026-04-01T00:00:00"}],
            [{"user_name": "a", "status": "join"},
             {"user_name": "b", "status": "pending"},
             {"user_name": "c", "status": "join"}],
        ]
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.get("/events?year=2026&month=4")
        data = res.json()
        assert set(data[0]["participants"]) == {"a", "c"}
        assert data[0]["pending"] == ["b"]


# ============================================================
# 2. イベント作成  POST /events
# ============================================================

class TestCreateEvent:

    def _make_cur(self):
        cur = MagicMock()
        cur.__enter__ = lambda s: cur
        cur.__exit__ = MagicMock(return_value=False)
        created = {"id": 1, "title": "ABCゴルフ", "date": MagicMock(isoformat=lambda: "2026-05-01"),
                   "start_time": "08:00", "end_time": "12:00", "description": "メモ",
                   "user_name": "tamura", "color": "blue", "created_at": "2026-04-01T00:00:00"}
        cur.fetchone.return_value = created
        return cur

    def test_正常_全フィールド指定(self):
        cur = self._make_cur()
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.post("/events", json={
                "title": "ABCゴルフ", "date": "2026-05-01",
                "start_time": "08:00", "end_time": "12:00",
                "description": "メモ", "user_name": "tamura", "color": "blue"
            })
        assert res.status_code == 201
        assert res.json()["title"] == "ABCゴルフ"

    def test_正常_必須フィールドのみ(self):
        cur = MagicMock()
        cur.__enter__ = lambda s: cur
        cur.__exit__ = MagicMock(return_value=False)
        cur.fetchone.return_value = {
            "id": 2, "title": "最小イベント", "date": MagicMock(isoformat=lambda: "2026-05-02"),
            "start_time": None, "end_time": None, "description": None,
            "user_name": "yamada", "color": "blue", "created_at": "2026-04-01T00:00:00"
        }
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.post("/events", json={
                "title": "最小イベント", "date": "2026-05-02", "user_name": "yamada"
            })
        assert res.status_code == 201

    def test_異常_titleなし(self):
        res = client.post("/events", json={"date": "2026-05-01", "user_name": "tamura"})
        assert res.status_code == 422

    def test_異常_dateなし(self):
        res = client.post("/events", json={"title": "test", "user_name": "tamura"})
        assert res.status_code == 422

    def test_異常_user_nameなし(self):
        res = client.post("/events", json={"title": "test", "date": "2026-05-01"})
        assert res.status_code == 422

    def test_色のデフォルトはblue(self):
        cur = MagicMock()
        cur.__enter__ = lambda s: cur
        cur.__exit__ = MagicMock(return_value=False)
        cur.fetchone.return_value = {
            "id": 3, "title": "test", "date": MagicMock(isoformat=lambda: "2026-05-03"),
            "start_time": None, "end_time": None, "description": None,
            "user_name": "tamura", "color": "blue", "created_at": "2026-04-01T00:00:00"
        }
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.post("/events", json={"title": "test", "date": "2026-05-03", "user_name": "tamura"})
        assert res.status_code == 201
        assert res.json()["color"] == "blue"

    def test_tamura以外の作成者は参加者に自動追加される(self):
        cur = MagicMock()
        cur.__enter__ = lambda s: cur
        cur.__exit__ = MagicMock(return_value=False)
        cur.lastrowid = 10
        cur.fetchone.return_value = {
            "id": 10, "title": "ABCゴルフ", "date": MagicMock(isoformat=lambda: "2026-05-01"),
            "start_time": None, "end_time": None, "description": None,
            "user_name": "yamada", "color": "blue", "created_at": "2026-04-01T00:00:00"
        }
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.post("/events", json={
                "title": "ABCゴルフ", "date": "2026-05-01", "user_name": "yamada"
            })
        assert res.status_code == 201
        # INSERT IGNORE INTO participants が呼ばれていること
        calls = [str(c) for c in cur.execute.call_args_list]
        assert any("INSERT IGNORE INTO participants" in c for c in calls)

    def test_tamuraの作成者は参加者に自動追加されない(self):
        cur = MagicMock()
        cur.__enter__ = lambda s: cur
        cur.__exit__ = MagicMock(return_value=False)
        cur.lastrowid = 11
        cur.fetchone.return_value = {
            "id": 11, "title": "ABCゴルフ", "date": MagicMock(isoformat=lambda: "2026-05-01"),
            "start_time": None, "end_time": None, "description": None,
            "user_name": "tamura", "color": "blue", "created_at": "2026-04-01T00:00:00"
        }
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.post("/events", json={
                "title": "ABCゴルフ", "date": "2026-05-01", "user_name": "tamura"
            })
        assert res.status_code == 201
        calls = [str(c) for c in cur.execute.call_args_list]
        assert not any("INSERT IGNORE INTO participants" in c for c in calls)

    def test_TAMURA大文字も参加者に自動追加されない(self):
        cur = MagicMock()
        cur.__enter__ = lambda s: cur
        cur.__exit__ = MagicMock(return_value=False)
        cur.lastrowid = 12
        cur.fetchone.return_value = {
            "id": 12, "title": "ABCゴルフ", "date": MagicMock(isoformat=lambda: "2026-05-01"),
            "start_time": None, "end_time": None, "description": None,
            "user_name": "TAMURA", "color": "blue", "created_at": "2026-04-01T00:00:00"
        }
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.post("/events", json={
                "title": "ABCゴルフ", "date": "2026-05-01", "user_name": "TAMURA"
            })
        assert res.status_code == 201
        calls = [str(c) for c in cur.execute.call_args_list]
        assert not any("INSERT IGNORE INTO participants" in c for c in calls)


# ============================================================
# 3. イベント更新  PATCH /events/{event_id}
# ============================================================

class TestUpdateEvent:

    def test_正常_タイトル変更(self):
        original = {"id": 1, "title": "旧タイトル", "date": MagicMock(isoformat=lambda: "2026-05-01"),
                    "start_time": None, "end_time": None, "description": None,
                    "user_name": "tamura", "color": "blue", "created_at": "2026-04-01T00:00:00"}
        updated = {**original, "title": "新タイトル"}
        cur = MagicMock()
        cur.__enter__ = lambda s: cur
        cur.__exit__ = MagicMock(return_value=False)
        cur.fetchone.side_effect = [original, updated]
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.patch("/events/1", json={"title": "新タイトル"})
        assert res.status_code == 200
        assert res.json()["title"] == "新タイトル"

    def test_異常_存在しないイベント(self):
        cur = make_cursor(fetchone_val=None)
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.patch("/events/9999", json={"title": "test"})
        assert res.status_code == 404

    def test_正常_部分更新_他フィールドは保持(self):
        original = {"id": 1, "title": "元タイトル", "date": MagicMock(isoformat=lambda: "2026-05-01"),
                    "start_time": "09:00", "end_time": None, "description": None,
                    "user_name": "tamura", "color": "blue", "created_at": "2026-04-01T00:00:00"}
        updated = {**original, "color": "red"}
        cur = MagicMock()
        cur.__enter__ = lambda s: cur
        cur.__exit__ = MagicMock(return_value=False)
        cur.fetchone.side_effect = [original, updated]
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.patch("/events/1", json={"color": "red"})
        assert res.status_code == 200
        assert res.json()["color"] == "red"
        assert res.json()["title"] == "元タイトル"


# ============================================================
# 4. イベント削除  DELETE /events/{event_id}
# ============================================================

class TestDeleteEvent:

    def test_正常_削除成功(self):
        cur = make_cursor()
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.delete("/events/1")
        assert res.status_code == 204

    def test_存在しないIDでも204を返す(self):
        cur = make_cursor()
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.delete("/events/9999")
        assert res.status_code == 204


# ============================================================
# 5. 参加  POST /events/{event_id}/join
# ============================================================

class TestJoinEvent:

    def _make_join_cur(self, exists=True, participants=None, pending=None):
        cur = MagicMock()
        cur.__enter__ = lambda s: cur
        cur.__exit__ = MagicMock(return_value=False)
        cur.fetchone.return_value = {"id": 1} if exists else None
        cur.fetchall.return_value = (
            [{"user_name": p, "status": "join"} for p in (participants or [])] +
            [{"user_name": p, "status": "pending"} for p in (pending or [])]
        )
        return cur

    def test_正常_参加登録(self):
        cur = self._make_join_cur(participants=["tamura"])
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.post("/events/1/join", json={"user_name": "tamura", "status": "join"})
        assert res.status_code == 200
        assert "tamura" in res.json()["participants"]

    def test_正常_検討中ステータス(self):
        cur = self._make_join_cur(pending=["yamada"])
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.post("/events/1/join", json={"user_name": "yamada", "status": "pending"})
        assert res.status_code == 200
        assert "yamada" in res.json()["pending"]

    def test_異常_存在しないイベント(self):
        cur = self._make_join_cur(exists=False)
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.post("/events/9999/join", json={"user_name": "tamura"})
        assert res.status_code == 404

    def test_不正ステータスはjoinに正規化(self):
        cur = self._make_join_cur(participants=["tamura"])
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.post("/events/1/join", json={"user_name": "tamura", "status": "invalid"})
        assert res.status_code == 200

    def test_重複参加はupsertで上書き(self):
        cur = self._make_join_cur(participants=["tamura"])
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.post("/events/1/join", json={"user_name": "tamura", "status": "join"})
        assert res.status_code == 200


# ============================================================
# 6. 参加取消  DELETE /events/{event_id}/join
# ============================================================

class TestLeaveEvent:

    def test_正常_参加取消(self):
        cur = make_cursor(rows=[])
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.request("DELETE", "/events/1/join",
                                 content=json.dumps({"user_name": "tamura"}),
                                 headers={"Content-Type": "application/json"})
        assert res.status_code == 200
        assert res.json()["participants"] == []

    def test_参加していないユーザーの取消でもエラーなし(self):
        cur = make_cursor(rows=[])
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.request("DELETE", "/events/1/join",
                                 content=json.dumps({"user_name": "nobody"}),
                                 headers={"Content-Type": "application/json"})
        assert res.status_code == 200


# ============================================================
# 7. OGP取得  GET /og
# ============================================================

class TestGetOG:

    def test_正常_OGPメタタグ取得(self):
        html = """<html><head>
            <meta property="og:title" content="テストタイトル">
            <meta property="og:description" content="テスト説明">
            <meta property="og:image" content="https://example.com/img.jpg">
        </head><body></body></html>"""
        mock_res = MagicMock()
        mock_res.text = html
        mock_res.apparent_encoding = "utf-8"
        with patch("main.http_requests.get", return_value=mock_res):
            res = client.get("/og?url=https://example.com")
        assert res.status_code == 200
        data = res.json()
        assert data["title"] == "テストタイトル"
        assert data["description"] == "テスト説明"
        assert data["image"] == "https://example.com/img.jpg"

    def test_正常_OGPなしはtitleタグにフォールバック(self):
        html = "<html><head><title>ページタイトル</title></head><body></body></html>"
        mock_res = MagicMock()
        mock_res.text = html
        mock_res.apparent_encoding = "utf-8"
        with patch("main.http_requests.get", return_value=mock_res):
            res = client.get("/og?url=https://example.com")
        assert res.status_code == 200
        assert res.json()["title"] == "ページタイトル"

    def test_異常_URLフェッチ失敗(self):
        with patch("main.http_requests.get", side_effect=Exception("timeout")):
            res = client.get("/og?url=https://example.com")
        assert res.status_code == 400

    def test_異常_urlパラメータなし(self):
        res = client.get("/og")
        assert res.status_code == 422


# ============================================================
# 8. メディアアップロード  POST /events/{event_id}/media
# ============================================================

class TestUploadMedia:

    def _setup(self, event_exists=True):
        cur = MagicMock()
        cur.__enter__ = lambda s: cur
        cur.__exit__ = MagicMock(return_value=False)
        cur.fetchone.return_value = {"id": 1} if event_exists else None
        cur.lastrowid = 10
        mock_s3.put_object.return_value = {}
        mock_s3.generate_presigned_url.return_value = "https://s3.example.com/signed"
        return cur

    def test_正常_画像アップロード(self):
        cur = self._setup()
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.post(
                "/events/1/media",
                data={"user_name": "tamura", "comment": "良いラウンドでした"},
                files={"file": ("photo.jpg", b"fakeimagecontent", "image/jpeg")}
            )
        assert res.status_code == 201
        data = res.json()
        assert data["media_type"] == "image"
        assert data["file_name"] == "photo.jpg"

    def test_正常_動画アップロード(self):
        cur = self._setup()
        with patch("main.get_conn", return_value=make_conn(cur)), \
             patch("main.cv2", MagicMock(), create=True):
            res = client.post(
                "/events/1/media",
                data={"user_name": "tamura"},
                files={"file": ("video.mp4", b"fakevideocontent", "video/mp4")}
            )
        assert res.status_code == 201
        assert res.json()["media_type"] == "video"

    def test_正常_コメントなし(self):
        cur = self._setup()
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.post(
                "/events/1/media",
                data={"user_name": "tamura"},
                files={"file": ("photo.png", b"fakecontent", "image/png")}
            )
        assert res.status_code == 201

    def test_異常_存在しないイベント(self):
        cur = self._setup(event_exists=False)
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.post(
                "/events/9999/media",
                data={"user_name": "tamura"},
                files={"file": ("photo.jpg", b"content", "image/jpeg")}
            )
        assert res.status_code == 404

    def test_動画拡張子判定_mp4(self):
        cur = self._setup()
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.post(
                "/events/1/media",
                data={"user_name": "tamura"},
                files={"file": ("clip.mp4", b"content", "video/mp4")}
            )
        assert res.json()["media_type"] == "video"

    def test_動画拡張子判定_mov(self):
        cur = self._setup()
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.post(
                "/events/1/media",
                data={"user_name": "tamura"},
                files={"file": ("clip.mov", b"content", "video/quicktime")}
            )
        assert res.json()["media_type"] == "video"

    def test_画像拡張子判定_png(self):
        cur = self._setup()
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.post(
                "/events/1/media",
                data={"user_name": "tamura"},
                files={"file": ("img.png", b"content", "image/png")}
            )
        assert res.json()["media_type"] == "image"


# ============================================================
# 9. メディア一覧取得  GET /events/{event_id}/media
# ============================================================

class TestListMedia:

    def test_正常_メディア一覧返却(self):
        cur = make_cursor(rows=[
            {"id": 1, "event_id": 1, "user_name": "tamura", "s3_key": "events/1/abc.jpg",
             "file_name": "photo.jpg", "media_type": "image", "comment": "コメント",
             "thumbnail_key": None, "created_at": MagicMock(isoformat=lambda: "2026-04-01T10:00:00")}
        ])
        mock_s3.generate_presigned_url.return_value = "https://s3.example.com/signed"
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.get("/events/1/media")
        assert res.status_code == 200
        data = res.json()
        assert len(data) == 1
        assert data[0]["file_name"] == "photo.jpg"
        assert "url" in data[0]

    def test_正常_メディアなし(self):
        cur = make_cursor(rows=[])
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.get("/events/1/media")
        assert res.status_code == 200
        assert res.json() == []

    def test_サムネイルURLが含まれる(self):
        cur = make_cursor(rows=[
            {"id": 1, "event_id": 1, "user_name": "tamura", "s3_key": "events/1/vid.mp4",
             "file_name": "vid.mp4", "media_type": "video", "comment": None,
             "thumbnail_key": "events/1/thumbs/thumb.jpg",
             "created_at": MagicMock(isoformat=lambda: "2026-04-01T10:00:00")}
        ])
        mock_s3.generate_presigned_url.return_value = "https://s3.example.com/signed"
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.get("/events/1/media")
        assert res.json()[0]["thumbnail_url"] is not None


# ============================================================
# 10. メディア削除  DELETE /media/{media_id}
# ============================================================

class TestDeleteMedia:

    def test_正常_削除成功(self):
        cur = MagicMock()
        cur.__enter__ = lambda s: cur
        cur.__exit__ = MagicMock(return_value=False)
        cur.fetchone.return_value = {"s3_key": "events/1/abc.jpg"}
        mock_s3.delete_object.return_value = {}
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.delete("/media/1")
        assert res.status_code == 204

    def test_異常_存在しないメディア(self):
        cur = make_cursor(fetchone_val=None)
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.delete("/media/9999")
        assert res.status_code == 404


# ============================================================
# 11. アルバム一覧  GET /album
# ============================================================

class TestGetAlbum:

    def test_正常_全イベント返却(self):
        cur = MagicMock()
        cur.__enter__ = lambda s: cur
        cur.__exit__ = MagicMock(return_value=False)
        cur.fetchall.side_effect = [
            [{"id": 1, "title": "ラウンド", "date": MagicMock(isoformat=lambda: "2026-03-01"),
              "start_time": None, "end_time": None, "description": None,
              "user_name": "tamura", "color": "blue", "created_at": "2026-03-01T00:00:00",
              "media_count": 5}],
            [{"user_name": "tamura", "status": "join"}],
        ]
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.get("/album")
        assert res.status_code == 200
        data = res.json()
        assert len(data) == 1
        assert data[0]["media_count"] == 5
        assert "tamura" in data[0]["participants"]

    def test_正常_イベントなしは空配列(self):
        cur = make_cursor(rows=[])
        with patch("main.get_conn", return_value=make_conn(cur)):
            res = client.get("/album")
        assert res.status_code == 200
        assert res.json() == []
