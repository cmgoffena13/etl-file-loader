from src.logging_conf import setup_logging
from src.process.processor import Processor

if __name__ == "__main__":
    setup_logging()
    processor = Processor()
    processor.process_files_in_parallel()
