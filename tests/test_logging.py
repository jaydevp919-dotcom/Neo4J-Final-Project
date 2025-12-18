from src.utils.logging_config import setup_logging

def test_logger_creates() -> None:
    logger = setup_logging()
    logger.info("test")
    assert logger.name == "pipeline"
