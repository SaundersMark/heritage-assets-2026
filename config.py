"""Configuration for Heritage Assets application"""

from pathlib import Path

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""

    # Database
    database_url: str = "sqlite:///heritage_assets.db"

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_key: str = ""  # For authenticated endpoints like /scrape

    # Scraping
    scrape_delay: float = 0.1
    scrape_detail_delay: float = 0.5
    scrape_timeout: int = 30
    scrape_max_workers: int = 5
    scrape_batch_size: int = 100

    # HMRC URLs
    hmrc_summary_url: str = (
        "http://www.visitukheritage.gov.uk/servlet/"
        "com.eds.ir.cto.servlet.CtoDbQueryServlet?"
        "location=All&class1=All&freetext=&Submit=search"
    )
    hmrc_detail_url_template: str = (
        "http://www.visitukheritage.gov.uk/servlet/"
        "com.eds.ir.cto.servlet.CtoDetailServlet?ID={unique_id}"
    )

    # Paths
    data_dir: Path = Path("data")
    logs_dir: Path = Path("logs")

    class Config:
        env_file = ".env"
        env_prefix = "HERITAGE_"


settings = Settings()
