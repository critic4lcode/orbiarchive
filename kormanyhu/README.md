# kormanyhu

Scrapes and downloads document archives from the Hungarian government portal ([kormany.hu](https://kormany.hu)).

## Requirements

- Python 3.10+
- `requests` — `pip install requests`

## Scripts

### `dokutar.py` — Fetch document group index

Calls the `kormany.hu` document-groups API and saves the full index to `all_document_groups.json`.

```bash
python dokutar.py
```

Paginates through all ~5 100 document groups with retry logic and a delay between requests.

### `download_docs.py` — Download documents

Downloads the actual ZIP archives for each document group using the index produced by `dokutar.py`.

```bash
python download_docs.py [options]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--input` | `all_document_groups.json` | Index file from `dokutar.py` |
| `--output-dir` | `downloads/` | Where to save ZIP files |
| `--workers` | `4` | Parallel download threads |

## Output

| Path | Description |
|------|-------------|
| `all_document_groups.json` | Full document group index from the API |
| `downloads/<slug>.zip` | Downloaded document archives |
