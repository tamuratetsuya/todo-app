# golfspace 開発ルール

## デプロイフロー（必ず守る）

コードを修正したら、以下の順番で進める。各ステップで確認不要。

1. **修正** — コード変更
2. **テスト** — テストケースで動作確認
3. **stgデプロイ** — ステージング環境に反映してURLを伝える
4. **人が確認** — ユーザーが stg で動作確認
5. **OKをもらう** — ユーザーから承認を受ける
6. **git push + 本番デプロイ** — 承認後に本番反映

本番への直接デプロイは禁止。必ずstgで確認を挟む。

---

## インフラ

| 項目 | 値 |
|------|-----|
| EC2 IP | 57.182.38.255（ElasticIP・固定） |
| SSHキー | /Users/tamura/claude_code/tododb.pem |
| 本番HTML | /usr/share/nginx/html/ |
| stg HTML | /usr/share/nginx/html/stg/ |
| calendar本番バックエンド | ~/calendar-backend/ ポート8002 |
| calendar stgバックエンド | ~/calendar-backend-staging/ ポート8012 |
| blog本番バックエンド | ~/blog-backend/ ポート8001 |
| blog stgバックエンド | ~/blog-backend-staging/ ポート8011 |
| 本番DB | tododb / blogdb |
| stg DB | stagingdb |
| S3 | golfspace-media (ap-northeast-1) |

### デプロイコマンド

```bash
# stgデプロイ（HTMLのみ）※必ずURLを置換してからデプロイ
sed -e "s|https://calendar.golfspace.jp/api/calendar|https://stg.calendar.golfspace.jp/api/calendar|g" \
    -e "s|https://calendar.golfspace.jp\"|https://stg.calendar.golfspace.jp\"|g" \
    <file> > /tmp/<file_stg>
scp -i /Users/tamura/claude_code/tododb.pem -o StrictHostKeyChecking=no /tmp/<file_stg> ec2-user@57.182.38.255:/tmp/<file>
ssh -i /Users/tamura/claude_code/tododb.pem -o StrictHostKeyChecking=no ec2-user@57.182.38.255 "sudo cp /tmp/<file> /usr/share/nginx/html/stg/"

# stgデプロイ（calendarバックエンド）
scp ... main.py ec2-user@57.182.38.255:/tmp/main.py
ssh ... "cp /tmp/main.py ~/calendar-backend-staging/main.py && sudo systemctl restart calendar-backend-staging"

# 本番デプロイ（HTMLのみ）
ssh ... "sudo cp /tmp/<file> /usr/share/nginx/html/"

# 本番デプロイ（calendarバックエンド）
ssh ... "cp /tmp/main.py ~/calendar-backend/main.py && sudo systemctl restart calendar-backend"

# git push
cd /Users/tamura/claude_code && git add -A && git commit -m "..." && git push origin main
```

## 必須ルール: 本番デプロイ前に必ずgit commit & push

本番（/usr/share/nginx/html/）へのデプロイは、必ず以下の順番で行う：

1. git add → git commit → git push
2. 本番デプロイ

stgデプロイはgit不要。本番デプロイ時のみ必須。どのPCからの作業でも同じルール。

---

## 承認済み仕様（変更不要）

### カレンダーアプリ（calendar.golfspace.jp）

- **アップロードUX**：ファイル選択 → ステージング表示＋コメント入力 → 「アップロードする」ボタンで確定（即時送信禁止）
- **コメント**：写真・動画にコメント添付可。サムネイルとライトボックスに表示
- **削除権限**：アップロード本人のみ。`tamura` は全件削除可能
- **時刻入力**：時(5〜20) + 分(10分刻み) の2セレクト式
- **タイトルサジェスト**：空欄時は頻度上位5件＋「この日ゴルフしたい」固定。入力中は部分一致フィルタ
- **動画サムネイル**：アップロード時にOpenCVで初フレーム抽出→S3保存。アルバムはJPEG表示（動画読み込みなし）

### 確認不要な操作

以下はユーザーへの確認なしで実行してよい：
- EC2へのデプロイ（scp + ssh）
- git commit & push
- systemctl restart
- DBデータの全削除（ユーザーが明示的に指示した場合）
- S3ファイルの全削除（ユーザーが明示的に指示した場合）
