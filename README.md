# Automated Data Crawler 🕷️

A high-performance, automated data crawling and validation pipeline designed for research and data analysis projects. This system specifically targets social platforms and forums to extract discussions around green energy and transportation transitions (ViEVPolicy project).

## 🚀 Features

- **X (Twitter) Scraper**: Asynchronous, concurrent scraping using Playwright with anti-detection measures.
- **Otofun Forum Crawler**: Robust extraction of forum threads and comments with automated deduplication.
- **Airflow Orchestration**: Scheduled pipelines for daily data collection.
- **Gemini Validation**: AI-powered content filtering to ensure data relevance using Google's Gemini models.
- **Deduplication**: Content hashing to prevent redundant data storage.

## 🏗️ Architecture

The project consists of three main layers:

1.  **Ingestion Layer**: `crawlers/` directory containing platform-specific scrapers.
2.  **Orchestration Layer**: `dags/` directory with Apache Airflow DAGs for scheduling.
3.  **Validation Layer**: `utils/gemini_validator.py` for intelligent content filtering.

## 📦 Installation

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/yourusername/automated-data-crawler.git
    cd automated-data-crawler
    ```

2.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

3.  **Setup Playwright**:
    ```bash
    playwright install chromium
    ```

4.  **Configuration**:
    Create a `.env` file in the root directory:
    ```env
    GEMINI_API_KEY=your_api_key_here
    ```

## 🛠️ Usage

### Running Crawlers Manually

You can use `main.py` as an entry point:

**Run X Scraper:**
```bash
python main.py x
```

**Run Otofun Scraper:**
```bash
python main.py otofun --url "SEARCH_URL" --output "results.csv"
```

### Airflow Pipeline

The daily crawl pipeline is defined in `dags/crawl_pipeline.py`. To run with Airflow:
1.  Ensure Airflow is installed and configured.
2.  Copy the `dags/` and project folders to your `AIRFLOW_HOME`.
3.  Enable the `daily_crawl_pipeline` DAG in the Airflow UI.

## 🤖 Data Validation

The `gemini_validator.py` script uses LLMs to filter relevant posts. It requires a `.env` file with a valid `GEMINI_API_KEY`.

```bash
python utils/gemini_validator.py
```

## 📝 License

MIT
