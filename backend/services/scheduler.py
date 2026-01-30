from apscheduler.schedulers.background import BackgroundScheduler
from loguru import logger

class SchedulerService:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SchedulerService, cls).__new__(cls)
            cls._instance.scheduler = BackgroundScheduler()
            cls._instance.started = False
        return cls._instance

    def start(self):
        if not self.started:
            self.scheduler.start()
            self.started = True
            logger.info("Scheduler service started")

    def stop(self):
        if self.started:
            self.scheduler.shutdown()
            self.started = False
            logger.info("Scheduler service stopped")

scheduler = SchedulerService()
