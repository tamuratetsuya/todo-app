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

## バージョン管理ルール

### バージョン番号の付け方
- `VERSION` ファイルで管理（例: `1.2.0`）
- セマンティックバージョニング: `メジャー.マイナー.パッチ`
  - パッチ（0.0.x）: バグ修正
  - マイナー（0.x.0）: 新機能追加
  - メジャー（x.0.0）: 大規模変更

### 本番デプロイ時の必須手順（どのPCでも同じ）

**ステップ1: バージョンチェック（必須）**
```bash
bash scripts/check_version.sh
# 本番より古いバージョンの場合は自動停止 → git pull して確認
```

**ステップ2: バージョン番号を上げる**
```bash
# VERSION ファイルを編集して新バージョンに更新（例: 1.0.0 → 1.1.0）
echo "1.1.0" > VERSION
```

**ステップ3: git commit & tag & push**
```bash
VERSION=$(cat VERSION)
git add -A
git commit -m "Release v$VERSION: 変更内容"
git tag -a "v$VERSION" -m "変更内容"
git push origin main
git push origin "v$VERSION"
```

**ステップ4: 本番デプロイ + S3にバージョン記録**
```bash
# ファイルをS3にアップ → SSMでEC2に反映
aws s3 cp VERSION s3://golfspace-media/deploy/DEPLOYED_VERSION  # ← 必ずデプロイ後に実行
```

### ロールバック手順
```bash
# タグ一覧確認
git tag -l | sort -V

# 特定バージョンのファイルを取得してデプロイ
TARGET=v1.0.0
git show $TARGET:stock.html > /tmp/stock.html
git show $TARGET:screening.html > /tmp/screening.html
git show $TARGET:stock-backend/main.py > /tmp/main.py

# S3経由でEC2にデプロイ
aws s3 cp /tmp/stock.html s3://golfspace-media/deploy/stock.html
aws s3 cp /tmp/screening.html s3://golfspace-media/deploy/screening.html
aws s3 cp /tmp/main.py s3://golfspace-media/deploy/main.py
# → SSMで本番反映後、S3のDEPLOYED_VERSIONも更新
echo $TARGET | sed 's/v//' | aws s3 cp - s3://golfspace-media/deploy/DEPLOYED_VERSION
```

---

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
