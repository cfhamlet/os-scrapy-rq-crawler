import logging

from scrapy.crawler import Crawler as ScrapyCrawler
from scrapy.settings import Settings
from scrapy.utils.misc import load_object

logger = logging.getLogger(__name__)


class Crawler(ScrapyCrawler):
    default_settings = {}

    def __init__(self, spidercls, settings=None):
        if isinstance(settings, dict) or settings is None:
            settings = Settings(settings)
        settings.setdict(self.default_settings, "default")
        super(Crawler, self).__init__(spidercls, settings)

    def _create_engine(self):
        engine_class_path = self.settings.get("ENGINE", None)
        engine = None
        if engine_class_path is None:
            engine = super(Crawler, self)._create_engine()
        else:
            engine_class = load_object(engine_class_path)
            engine = engine_class(self, lambda _: self.stop())
        p = f"{engine.__module__}.{engine.__class__.__name__}"
        logger.debug(f"Using engine: {p}")
        return engine
