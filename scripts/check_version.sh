#!/bin/bash
# 本番デプロイ前バージョンチェック
# 本番より古いバージョンをデプロイしようとした場合に警告・停止する

LOCAL_VERSION=$(cat "$(dirname "$0")/../VERSION" | tr -d '[:space:]')
DEPLOYED_VERSION=$(aws s3 cp s3://golfspace-media/deploy/DEPLOYED_VERSION - 2>/dev/null | tr -d '[:space:]')

if [ -z "$DEPLOYED_VERSION" ]; then
  echo "⚠️  本番バージョンを取得できませんでした。デプロイを続行しますか？ [y/N]"
  read -r answer
  [[ "$answer" =~ ^[Yy]$ ]] || exit 1
  exit 0
fi

echo "ローカルバージョン : $LOCAL_VERSION"
echo "本番デプロイ済み  : $DEPLOYED_VERSION"

# バージョン比較（major.minor.patch）
version_gt() {
  # $1 > $2 なら 0 を返す
  [ "$1" = "$2" ] && return 1
  local IFS=.
  local i a=($1) b=($2)
  for ((i=0; i<${#a[@]}; i++)); do
    if ((${a[i]} > ${b[i]:-0})); then return 0; fi
    if ((${a[i]} < ${b[i]:-0})); then return 1; fi
  done
  return 1
}

if version_gt "$DEPLOYED_VERSION" "$LOCAL_VERSION"; then
  echo ""
  echo "🚨 警告: 本番 ($DEPLOYED_VERSION) よりも古いバージョン ($LOCAL_VERSION) をデプロイしようとしています！"
  echo "   別のPCで新しいバージョンがデプロイされている可能性があります。"
  echo "   最新のgitをpullしてバージョンを確認してください。"
  echo ""
  echo "   それでもデプロイしますか？ [y/N]"
  read -r answer
  [[ "$answer" =~ ^[Yy]$ ]] || { echo "デプロイを中止しました。"; exit 1; }
elif [ "$LOCAL_VERSION" = "$DEPLOYED_VERSION" ]; then
  echo "✅ バージョン一致 (v$LOCAL_VERSION) — デプロイ可能"
else
  echo "✅ ローカル (v$LOCAL_VERSION) > 本番 (v$DEPLOYED_VERSION) — デプロイ可能"
fi

exit 0
