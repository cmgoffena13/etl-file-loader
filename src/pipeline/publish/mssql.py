from sqlalchemy import Engine

from src.pipeline.publish.base import BasePublisher
from src.sources.base import DataSource


class SQLServerPublisher(BasePublisher):
    def __init__(self, source: DataSource, engine: Engine, log_id: int):
        super().__init__(source, engine, log_id)

    def create_publish_sql(self):
        pass

    def publish_data(self):
        pass
