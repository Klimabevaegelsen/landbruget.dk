# dyrenesdetektiv_scraper

Scrapes Danish animal-welfare control records from
[dyrenesdetektiv.dk](https://dyrenesdetektiv.dk/kontrol/), which republishes
inspection summaries originally produced by **Fødevarestyrelsen** (the Danish
Veterinary and Food Administration). Joinable to the rest of Landbruget on CHR
and CVR.

## Stages

- **Bronze** — paginates the WordPress REST API for the `kontrol` custom post
  type (~15 calls of 100 records each) and downloads each detail HTML page
  (~1479 records). Throttled at ~1 req/s with a polite `User-Agent`.
- **Silver** — parses each detail HTML with BeautifulSoup, validates CHR
  (`^\d{6}$`) and CVR (`^\d{8}$`), normalises dates, derives `sanktion_ordinal`
  (1–4), and writes a single Parquet file via DuckDB.

## Run

```bash
cd backend/pipelines/dyrenesdetektiv_scraper

# Bronze + silver against ./data, capped to 5 detail pages for smoke testing.
uv run python main.py --stage all --limit 5

# Full bronze run (~25 minutes).
uv run python main.py --stage bronze

# Re-parse the latest bronze run.
uv run python main.py --stage silver
```

Set `ENVIRONMENT=production` plus `R2_BUCKET` / `R2_ACCESS_KEY_ID` /
`R2_SECRET_ACCESS_KEY` / `R2_ACCOUNT_ID` to upload to R2 instead of local
storage.

## Silver schema

| Column             | Type    | Notes                                                      |
| ------------------ | ------- | ---------------------------------------------------------- |
| `kontrol_id`       | BIGINT    | WP post id, primary key                                          |
| `link`             | VARCHAR   | Canonical detail page URL                                        |
| `slug`             | VARCHAR   | URL slug                                                         |
| `published_at`     | TIMESTAMP | WP post date                                                     |
| `modified_at`      | TIMESTAMP | WP post modified date                                            |
| `sagsnummer`       | VARCHAR   | Fødevarestyrelsen case ID (often blank)                          |
| `kontrol_dato`     | DATE      | Inspection date                                                  |
| `dyreart`          | VARCHAR   | Animal type / housing system                                     |
| `antal_dyr`        | BIGINT    | Animal count (nullable)                                          |
| `aarsag`           | VARCHAR   | Reason (often blank)                                             |
| `by`               | VARCHAR   | City / kommune string                                            |
| `chr_nummer`       | VARCHAR   | 6-digit zero-padded; null when redacted                          |
| `cvr_nummer`       | VARCHAR   | 8-digit zero-padded; null when redacted                          |
| `sanktion`         | VARCHAR   | Sanction label (e.g. "Indskærpelse")                             |
| `sanktion_ordinal` | INTEGER   | 1=Ingen anmærkninger, 2=Indskærpelse, 3=Politianmeldelse, 4=Bøde |
| `kontroltekst`     | VARCHAR   | Free-text findings narrative                                     |
| `tag_year`         | VARCHAR   | Year (4 digits); from `kontrol_tag` taxonomy or URL-slug fallback |
| `tag_kommune`      | VARCHAR   | Kommune name from `kontrol_tag` taxonomy                          |
| `tag_dyreart`      | VARCHAR   | Animal type from `kontrol_tag` taxonomy (Svin, Kvæg, Hunde, …)    |
| `parsed_at`        | TIMESTAMP | When this silver parse ran                                       |

## Source

- Site: <https://dyrenesdetektiv.dk/>
- Upstream authority: Fødevarestyrelsen (FVST)

## Permission to scrape

Landbruget.dk has obtained explicit permission from dyrenesdetektiv.dk to
scrape this data. **Anyone forking or reusing this pipeline must obtain
their own permission from dyrenesdetektiv.dk before running it** — the
site's open `robots.txt` and public licence do not waive that. Be polite
(1 req/s, identifying `User-Agent`).
