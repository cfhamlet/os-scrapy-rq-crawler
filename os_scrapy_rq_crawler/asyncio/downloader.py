from scrapy.core.downloader import Downloader as ScrapyDownloader


class Downloader(ScrapyDownloader):
    def _process_queue(self, spider, slot):
        while slot.queue and slot.free_transfer_slots() > 0:
            request, deferred = slot.queue.popleft()
            dfd = self._download(slot, request, spider)
            dfd.chainDeferred(deferred)
