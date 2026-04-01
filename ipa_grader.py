"""
IPA要件定義ガイド 問題採点システム
Claude APIを使ってテキスト回答を正誤判定する
"""

import anthropic
import json

# ===== 問題と採点基準の定義 =====

QUESTION = """
IPAが発行した「ユーザのための要件定義ガイド 第2版」では、ITシステムを「守りの分野」と「攻めの分野」に分けて説明しています。

(1) 「守りの分野」と「攻めの分野」それぞれの特徴を、具体的な技術例を挙げながら100字程度で説明してください。

(2) 「守りの分野」における代表的な課題として「レガシー化」が挙げられています。レガシー化が発生する主な原因と、それがシステム再構築に与える困難を説明してください。

(3) ガイドでは、ユーザ企業の経営層がシステム開発の上流工程により深く関与すべきと述べています。その理由を、競争力強化の観点から述べてください。
"""

SCORING_CRITERIA = """
## 採点基準

### (1) 守りの分野・攻めの分野の特徴 (30点)
- 守りの分野：基幹業務に必要なデータの転送・蓄積・処理が中心であることに言及 (10点)
- 攻めの分野：IoT・ビッグデータ・AIなど新技術を活用し、顧客との協働・新サービス提供に言及 (10点)
- 両分野の連携が経営課題になっているという視点 (10点)

### (2) レガシー化の原因と困難 (40点)
- 原因：長年の保守による業務仕様の理解不足、ハード・ソフト・技術の時代遅れ化 (20点)
- 困難：再構築時のコスト超過・稼動延伸リスク、ブラックボックス化による把握困難 (20点)

### (3) 経営層関与の理由 (30点)
- 「現行通りで」等のあいまいな要求ではなく、何をどうしたいかを明確化することが重要 (15点)
- 上流工程への関与が企業の競争力強化につながるという論述 (15点)
"""


def grade_answer(answer_text: str) -> dict:
    """
    Claude APIを使って回答を採点する

    Returns:
        dict: {
            "total_score": int,        # 合計点 (0-100)
            "sub_scores": dict,        # 各問の点数
            "feedback": str,           # 全体フィードバック
            "details": dict,           # 各問の詳細評価
        }
    """
    client = anthropic.Anthropic()

    prompt = f"""あなたはITシステム開発・要件定義の専門家として、以下の問題に対する回答を採点してください。

## 問題文
{QUESTION}

## 採点基準
{SCORING_CRITERIA}

## 受験者の回答
{answer_text}

## 採点指示
上記の採点基準に従って以下のJSON形式で採点結果を返してください。
JSONのみを返し、前後に余計なテキストを含めないでください。

{{
  "total_score": <合計点 0-100>,
  "sub_scores": {{
    "q1": <(1)の点数 0-30>,
    "q2": <(2)の点数 0-40>,
    "q3": <(3)の点数 0-30>
  }},
  "details": {{
    "q1": {{
      "score": <点数>,
      "good_points": "<良かった点>",
      "missing_points": "<不足していた点>"
    }},
    "q2": {{
      "score": <点数>,
      "good_points": "<良かった点>",
      "missing_points": "<不足していた点>"
    }},
    "q3": {{
      "score": <点数>,
      "good_points": "<良かった点>",
      "missing_points": "<不足していた点>"
    }}
  }},
  "feedback": "<全体的なフィードバック（200字以内）>"
}}"""

    response = client.messages.create(
        model="claude-opus-4-6",
        max_tokens=2000,
        messages=[{"role": "user", "content": prompt}],
    )

    result_text = response.content[0].text.strip()

    # JSON部分を抽出（前後に余計なテキストがある場合に対応）
    start = result_text.find("{")
    end = result_text.rfind("}") + 1
    if start != -1 and end > start:
        result_text = result_text[start:end]

    return json.loads(result_text)


def display_result(result: dict):
    """採点結果を見やすく表示する"""
    print("\n" + "=" * 60)
    print("採点結果")
    print("=" * 60)
    print(f"\n【合計点】 {result['total_score']} / 100 点\n")

    print("【各問の得点】")
    sub = result["sub_scores"]
    print(f"  (1) 守り・攻めの分野の特徴:  {sub['q1']} / 30 点")
    print(f"  (2) レガシー化の原因と困難:  {sub['q2']} / 40 点")
    print(f"  (3) 経営層関与の理由:        {sub['q3']} / 30 点")

    print("\n【各問の詳細評価】")
    for key, label in [("q1", "(1)"), ("q2", "(2)"), ("q3", "(3)")]:
        d = result["details"][key]
        print(f"\n  {label}  {d['score']}点")
        print(f"  ✓ 良かった点: {d['good_points']}")
        print(f"  △ 不足点:     {d['missing_points']}")

    print(f"\n【総評】\n{result['feedback']}")
    print("\n" + "=" * 60)


def grade_interactive():
    """対話形式で回答を入力して採点する"""
    print("=" * 60)
    print("IPA 要件定義ガイド 採点システム")
    print("=" * 60)
    print("\n以下の問いすべてに回答してください。")
    print("入力完了後、空行を2回続けて押してください。\n")
    print(QUESTION)
    print("-" * 60)

    lines = []
    empty_count = 0
    while True:
        line = input()
        if line == "":
            empty_count += 1
            if empty_count >= 2:
                break
        else:
            empty_count = 0
        lines.append(line)

    answer = "\n".join(lines).strip()
    if not answer:
        print("回答が入力されていません。")
        return

    print("\n採点中です...")
    try:
        result = grade_answer(answer)
        display_result(result)
    except json.JSONDecodeError as e:
        print(f"採点結果のパースエラー: {e}")
    except anthropic.APIError as e:
        print(f"API エラー: {e}")


def grade_sample():
    """サンプル回答で動作確認する"""
    sample_answer = """
(1) 守りの分野は基幹業務システムでデータ処理・蓄積・転送が中心です。攻めの分野はIoT・ビッグデータ・AIなど
新技術を活用し、顧客との協働による新サービス提供を重視します。両分野の連携が経営課題となっています。

(2) レガシー化は、長期間の保守運用によって業務仕様の理解が失われ、ハードウェアやソフトウェアが時代遅れに
なることで発生します。再構築時には、ブラックボックス化した仕様の解読が困難で、コスト超過や稼動延伸の
リスクが高まります。

(3) 経営層が上流工程に関与することで、「現行通りで」といったあいまいな要求ではなく、何をどうしたいかを
明確に定義できます。これにより要件定義の品質が向上し、企業の競争力強化につながります。
"""
    print("【サンプル回答での動作確認】")
    print("回答内容:")
    print(sample_answer)
    print("\n採点中...")

    result = grade_answer(sample_answer)
    display_result(result)
    return result


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--sample":
        grade_sample()
    else:
        grade_interactive()
