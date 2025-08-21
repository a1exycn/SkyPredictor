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
import traceback
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime

from baseline.fetcher import Fetcher
from baseline.predict import PredictService, PredictConfig
from baseline.result import ResultManager

log = logging.getLogger(__name__)


class Calculator:

    def __init__(self, conf: PredictConfig, fetcher: Fetcher, saver: ResultManager):
        self.conf = conf
        self.fetcher = fetcher
        self.saver = saver


    def start(self, start_time : datetime = datetime.now()):
        """
        Start the baseline calculation process.
        :param datetime: The reference starting datetime to use for all baseline calculations, metrics will be \
            retrieved and predicted based on this datetime.
        """

        # clear the timezone info as this is the common datetime format used in the application
        if start_time.tzinfo:
            start_time = start_time.replace(tzinfo=None)

        # dictionary to store our futures (prediction tasks); maps a future to a meter name
        futures = {}

        with ProcessPoolExecutor() as executor:

            # obtain metric names from OAP service with fetcher
            metric_names = self.fetcher.metric_names()

            # deal with empty metrics
            if not metric_names:
                log.error("No metric names found")
                return

            # try to prepare fetcher for fetching
            try:
                self.fetcher.ready_fetch(end_time=start_time)
            except Exception as e:
                log.error(f"Ready to fetch data failure, skip calculate predict: {e}, stacktrace: {"".join(traceback.format_exception(type(e), e, e.__traceback__))}")
                return

            # for each meter, submit a prediction service and map the future to the meter name    
            for inx in range(len(metric_names)):
                meter = metric_names[inx]
                log.info("Calculating baseline for %s" % meter)

                future = executor.submit(
                    PredictService(
                        fetcher=self.fetcher, 
                        name=meter, 
                        conf=self.conf, 
                        start_time=start_time
                    ).predict
                )

                futures[future] = meter

            while futures:

                # wait on at least one future to be completed and then group into two sets based on completion
                done, remaining_futures = wait(futures.keys(), return_when=FIRST_COMPLETED)

                # for each completed future, try to save the result using the results manager 
                for future in done:
                    try:
                        result = future.result()
                        meter = futures[future]
                        self.saver.save(meter, result)
                    except Exception as e:
                        log.error(f"Calculate or saving metrics failure: {e}, stacktrace: {"".join(traceback.format_exception(type(e), e, e.__traceback__))}")
                
                # update futures to only contain the incomplete futures
                futures = {future: futures[future] for future in remaining_futures}