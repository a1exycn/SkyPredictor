#  Copyright 2025 SkyAPM org
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import logging

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from datetime import datetime, timedelta

from baseline.calculate import Calculator
from baseline.fetcher import Fetcher
from baseline.predict import PredictConfig
from baseline.result import ResultManager

logger = logging.getLogger(__name__)


class Scheduler:
    def __init__(self, cron: str, conf: PredictConfig, fetcher: Fetcher, saver: ResultManager):
        self.cron = cron
        self.conf = conf
        self.fetcher = fetcher
        self.saver = saver


    def start(self):
        logger.info("Starting the dynamic baseline scheduler")
        scheduler = BackgroundScheduler()
        trigger = CronTrigger.from_crontab(self.cron)

        """
            run the job immediately with the previous day's datetime,
            this is done because we do not want to miss a baseline calculation if
            we run the scheduler late, relative to the cron schedule.
            we also do not consider the closest future time, as it will be run by
            the running program at the next scheduled time.
        """
        Calculator(self.conf, self.fetcher, self.saver).start(
            trigger.get_next_fire_time(
                None,
                datetime.now() - timedelta(days=1)
            )
        )

        scheduler.add_job(self.run_job, trigger)
        scheduler.start()


    def run_job(self):
        logger.info("Running the baseline calculation job")
        Calculator(self.conf, self.fetcher, self.saver).start()

