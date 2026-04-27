# Threads 自動投稿システム

GitHub Actions + Python + Notion API + Threads API 直接連携の実装です。

## 構成

- `threads_post.py`
  - Notion の投稿予約データソースを検索
  - `ステータス = 予約`
  - `投稿予定日時 <= 現在時刻`
  - `プラットフォーム = Threads`
  - Threads API に 2 段階で投稿
  - 成功時に Notion を `投稿済` に更新
  - 失敗時に Notion を `エラー` に更新

- `get_long_lived_token.py`
  - 短期トークンを長期トークンへ交換
  - 既存の長期トークンを refresh するモードも付属

- `.github/workflows/threads-post.yml`
  - 毎日 7:00 / 12:00 / 20:00 JST に実行

## 前提

- Notion の対象は `database_id` ではなく `data_source_id`
- Notion の該当データソースは、Integration に共有済みであること
- Threads アカウントは公開設定で、Instagram と連携済みであること
- Threads のテキスト投稿は 500 文字上限
- GitHub Actions の schedule は UTC 基準だが、workflow syntax で timezone を指定できる

## 1. GitHub リポジトリを作成する

1. GitHub にログインする
2. 右上の `+` をクリックする
3. `New repository` をクリックする
4. `Repository name` を入力する
5. `Public` を選ぶ
6. `Create repository` をクリックする

理由:
- Public repository の standard GitHub-hosted runner は free かつ unlimited で使えるため、完全無料運用に向く

## 2. ファイルを配置する

リポジトリ直下に以下を配置する。

- `threads_post.py`
- `get_long_lived_token.py`
- `requirements.txt`
- `.github/workflows/threads-post.yml`

## 3. Python 依存関係を入れる

`requirements.txt` を使って `requests` を入れる。

```bash
pip install -r requirements.txt
