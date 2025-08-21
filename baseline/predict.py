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

import os
import datetime
import logging
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import pandas as pd
from prometheus_client import Counter, Summary
from prophet import Prophet

from baseline.fetcher import LabelKeyValue, Fetcher, FetchedData

INCLUDE_HISTORICAL = os.getenv('INCLUDE_HISTORICAL', 'false').lower()  in ('true', '1', 't')

logger = logging.getLogger(__name__)

predict_total_count = Counter(
    'predict_total_count', 
    'The total number of predict metrics', 
    ['name']
)

predict_metrics_count = Counter(
    'predict_metrics_count', 
    'The number of predict metrics', 
    ['name']
)

predict_metrics_total_time = Summary(
    'predict_metrics_total_time',
    'The time spent on predict all metrics',
    ['name']
)

predict_metrics_group_metrics_time = Summary(
    'predict_metrics_group_metrics_time', 
    'The time spent on predict metrics',
    ['name']
)
predict_metrics_single_time = Summary(
    'predict_metrics_single_time',
    'The time spent on predict single metrics',
    ['name']
)


class PredictConfig:
    def __init__(self, min_days: int, frequency: str, period: int, offset: int = 0):
        self.min_days = min_days
        self.frequency = frequency
        self.period = period
        self.offset = offset


class ReadyPredictMeter:
    def __init__(
        self, 
        service_name: str, 
        single_df: Optional[pd.DataFrame] = None,
        label_dfs: Optional[dict[frozenset[LabelKeyValue], 
        pd.DataFrame]] = None
    ):
        self.service_name = service_name
        self.single_df = single_df
        self.label_dfs = label_dfs


class PredictValue:
    value: int
    upper_value: int
    lower_value: int

    def __init__(self, value: int, upper_value: int, lower_value: int):
        self.value = value
        self.upper_value = upper_value
        self.lower_value = lower_value

    @staticmethod
    def from_dict(d: dict) -> "PredictValue":
        return PredictValue(d["value"], d["upper_value"], d["lower_value"])


class PredictTimestampWithSingleValue:
    '''
    Represents a single timestamp prediction with its associated value.
    '''

    timestamp: pd.Timestamp
    value: PredictValue

    def __init__(self, timestamp: pd.Timestamp, value: PredictValue):
        self.timestamp = timestamp
        self.value = value

    @staticmethod
    def from_dict(d: dict) -> "PredictTimestampWithSingleValue":
        return PredictTimestampWithSingleValue(
            pd.Timestamp(d["timestamp"]),
            PredictValue.from_dict(d["value"])
        )


class PredictLabeledWithLabeledValue:
    '''
    Represents a labeled prediction, as defined by its frozenset, with its ENTIRE ASSOCIATED TIMESERIES
    Is not of the same concept as PredictTimestampWithSingleValue, which is a single point in time.
    Why they are not the same? I have no clue, please may the gods of code save us all.
    '''

    label: frozenset[LabelKeyValue]
    time_with_values: list[PredictTimestampWithSingleValue]

    def __init__(
        self, 
        label: frozenset[LabelKeyValue], 
        time_with_values: list[PredictTimestampWithSingleValue]
    ):
        self.label = label
        self.time_with_values = time_with_values

    @staticmethod
    def from_dict(d: dict) -> "PredictLabeledWithLabeledValue":
        label = frozenset(LabelKeyValue.from_dict(l) for l in d["label"])
        time_with_values = [
            PredictTimestampWithSingleValue.from_dict(twv)
            for twv in d["time_with_values"]
        ]
        return PredictLabeledWithLabeledValue(label, time_with_values)


class PredictMeterResult:
    service_name : str
    single : Optional[list[PredictTimestampWithSingleValue]]
    labeled : Optional[list[PredictLabeledWithLabeledValue]]
    historical : bool # Whether this result contains historical data

    def __init__(
        self,  
        service_name: str, 
        single: Optional[list[PredictTimestampWithSingleValue]] = None,
        labeled: Optional[list[PredictLabeledWithLabeledValue]] = None,
        historical: bool = False
    ):
        self.service_name = service_name
        self.single = single
        self.labeled = labeled
        self.historical = historical


    def filter_time(self, start: pd.Timestamp, end: pd.Timestamp):
        if self.single:
            self.single = [entry for entry in self.single if start <= entry.timestamp <= end]
        if self.labeled:
            for labeled_entry in self.labeled:
                labeled_entry.time_with_values = [
                    entry for entry in labeled_entry.time_with_values if start <= entry.timestamp <= end
                ]


    @staticmethod
    def from_dict(d: dict) -> "PredictMeterResult":
        single, labeled = None, None
        if d.get("single") is not None:
            single = [PredictTimestampWithSingleValue.from_dict(s) for s in d.get("single")]
        if d.get("labeled") is not None:
            labeled = [PredictLabeledWithLabeledValue.from_dict(l) for l in d.get("labeled")]
        return PredictMeterResult(d["service_name"], single, labeled)


def meter_to_result(
    meter : ReadyPredictMeter, 
    single : Optional[pd.DataFrame] = None,
    multiple : Optional[dict[frozenset[LabelKeyValue], pd.DataFrame]] = None
) -> PredictMeterResult:
    '''
    Convert raw Prophet prediction data into a structured PredictMeterResult, 
    which only includes the latest prediction data; data can be either single
    or multiple labelled.

    :param single: DataFrame with single value predictions
    :param multiple: Dictionary of DataFrames with multiple value predictions, keyed by labels
    :param include_historical: Whether to include historical data in the result
    '''

    """
    https://docs.victoriametrics.com/#data-updates
    VictoriaMetrics does not overwrite historical data! We will store historical data in a separate metric.
    """
    
    if single is not None:
        max_ds = meter.single_df['ds'].max()
        data = single[~single['ds'].isin(meter.single_df['ds']) | (single['ds'] == max_ds)]
        result: list[PredictTimestampWithSingleValue] = []
        for idx, row in data.iterrows():
            result.append(PredictTimestampWithSingleValue(
                timestamp=row['ds'],
                value=PredictValue(value=row['yhat'], upper_value=row['yhat_upper'], lower_value=row['yhat_lower'])
            ))
        return PredictMeterResult(meter.service_name, single=result)

    elif multiple is not None:
        result: list[PredictLabeledWithLabeledValue] = []
        for labels, label_df in meter.label_dfs.items():
            max_ds = label_df['ds'].max()
            data = multiple[labels][(~multiple[labels]['ds'].isin(label_df['ds'])) | (multiple[labels]['ds'] == max_ds)]
            time_with_values: list[PredictTimestampWithSingleValue] = []
            for idx, row in data.iterrows():
                time_with_values.append(PredictTimestampWithSingleValue(
                    timestamp=row['ds'],
                    value=PredictValue(
                        value=ignore_negative_value(row['yhat']),
                        upper_value=ignore_negative_value(row['yhat_upper']),
                        lower_value=ignore_negative_value(row['yhat_lower'])
                    )
                ))
            result.append(PredictLabeledWithLabeledValue(label=labels, time_with_values=time_with_values))
        return PredictMeterResult(meter.service_name, labeled=result)

    else:
        raise ValueError("Both single and multiple data are None. At least one must be provided.")


def meter_to_historical(
    meter: ReadyPredictMeter,
    single: Optional[pd.DataFrame] = None,
    multiple: Optional[dict[frozenset[LabelKeyValue], pd.DataFrame]] = None
) -> PredictMeterResult:

    if single is not None:
        result : list[PredictTimestampWithSingleValue] = []
        for _, row in single.iterrows():
            val = row['yhat']
            result.append(
                PredictTimestampWithSingleValue(
                    timestamp=row['ds'],
                    value=PredictValue(
                        lower_value=val,
                        value=val,
                        upper_value=val
                    )
                )
            )
        return PredictMeterResult(meter.service_name, single=result, historical=True)
    # end if single is not None

    elif multiple is not None:
        result: list[PredictLabeledWithLabeledValue] = []

        for labels, label_df in multiple.items():
            time_with_values: list[PredictTimestampWithSingleValue] = []

            for _, row in label_df.iterrows():
                val = row['yhat']
                time_with_values.append(
                    PredictTimestampWithSingleValue(
                        timestamp=row['ds'],
                        value=PredictValue(
                            lower_value=val,
                            value=val,
                            upper_value=val
                        )
                    )
                )
                
            result.append(PredictLabeledWithLabeledValue(label=labels, time_with_values=time_with_values))
        # end for labels, label_df in multiple.items()

        return PredictMeterResult(meter.service_name, labeled=result, historical=True)
    # end elif multiple is not None

    else:
        raise ValueError("Both single and multiple data are None. At least one must be provided.")



def ignore_negative_value(value: int) -> int:
    return value if value > 0 else 0


class PredictService:

    def __init__(self, fetcher: Fetcher, name: str, conf: PredictConfig, start_time : datetime.datetime):
        self.fetcher = fetcher
        self.name = name
        self.conf = conf
        self.start_time = start_time
        self.future_max_time = calc_max_predict_time(conf, start_time)


    def predict(self) -> list[PredictMeterResult]:

        # measure elapsed duration with Prometheus
        with predict_metrics_total_time.labels(self.name).time():
            return self.predict0()


    def predict0(self) -> list[PredictMeterResult]:

        # increase counter metric
        predict_total_count.labels(self.name).inc()

        # fetch the correct metric assigned to this
        data = self.fetcher.fetch(
            metric_name=self.name, 
            # end_time is start_time because start_time is the reference point for prediction, while end_time is the time to fetch data until
            end_time=self.start_time 
        )

        # deal with failed fetch / null metric or lack of data
        if data is None or len(data.df) == 0:
            logger.info(f"no data fetched for {self.name}")
            return []

        result: list[PredictMeterResult] = []
        
        # with predict_metrics_group_metrics_time.labels(self.name).time():

        #     # construct metrics from generator function, with one element per-service
        #     metrics = list(self.split_to_meter(data))

        # logger.info(f"total {len(metrics)} services in the {self.name} is available to calc baseline")
        start_time = time.perf_counter()
        # predict_metrics_count.labels(self.name).inc(len(metrics))

        # process each meter in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor() as executor:

            # submit each meter to the executor and collect futures
            # futures = [executor.submit(self.process_meter, meter) for idx, meter in enumerate(metrics, 1)]
            futures = []
            for metric in self.split_to_meter(data):
                # increase counter metric
                predict_metrics_count.labels(self.name).inc()

                # submit the meter to the executor
                futures.append(executor.submit(self.process_meter, metric))
            
            # wait for all futures to complete
            for future in futures:

                try:
                    if not INCLUDE_HISTORICAL:
                        result.append(future.result())

                    else:
                        for res in future.result():
                            result.append(res)

                except Exception as e:
                    logger.error(f"Error processing meter: {e}, stacktrace: {"".join(traceback.format_exception(type(e), e, e.__traceback__))}")
        
        end_time = time.perf_counter()
        logger.info(f"process {self.name} metrics total use time {end_time - start_time:.6f} seconds")
        return result


    def process_meter(
        self, 
        meter : ReadyPredictMeter
    ) -> PredictMeterResult | tuple[PredictMeterResult, PredictMeterResult]:
        '''
        Process a single meter, either single or multiple values, and return the result.

        :return: If INCLUDE_HISTORICAL is False, it will return only the prediction data,
        otherwise it will return both prediction and historical data in this order.
        '''

        with predict_metrics_single_time.labels(self.name).time():
            return self.process_meter0(meter)


    def process_meter0(
        self, 
        meter : ReadyPredictMeter
    ) -> PredictMeterResult | tuple[PredictMeterResult, PredictMeterResult]:


        def forecast_single_value(df : pd.DataFrame) -> pd.DataFrame:
            # generate Prophet model and fit it to the data
            m = Prophet(daily_seasonality=True, weekly_seasonality=False, yearly_seasonality=False)
            m.fit(df)
        
            # make future dataframe and predict
            future = m.make_future_dataframe(
                periods=self.calc_future_period(df), 
                freq=self.conf.frequency
            )

            future = m.predict(future)

            # Apply offset to timestamps based on configuration
            if self.conf.offset > 0:
                freq = self.conf.frequency.lower()
                if freq == 'd':
                    offset_delta = pd.Timedelta(days=self.conf.offset)
                elif freq == 'h':
                    offset_delta = pd.Timedelta(hours=self.conf.offset)
                elif freq == 'min' or freq == 'm' or freq == 't':
                    offset_delta = pd.Timedelta(minutes=self.conf.offset)
                elif freq == 's':
                    offset_delta = pd.Timedelta(seconds=self.conf.offset)
                elif freq == 'w':
                    offset_delta = pd.Timedelta(weeks=self.conf.offset)
                else:
                    raise ValueError(f"Unknown frequency type: {self.conf.frequency}")
                
                future['ds'] = future['ds'] + offset_delta

            logger.info(f"from {df['ds'].size} data points, predicted {future['ds'].size} future points for {meter.service_name} of {self.name}")
            return future


        # handle the case for a single metric per service (singlevalue)
        if meter.single_df is not None:

            # generate Prophet model and fit it to the data
            forecast = forecast_single_value(meter.single_df)

            # logger.info(f"Predicted for {meter.service_name} of {self.name} to {future["ds"].max()}")

            if not INCLUDE_HISTORICAL:
                return meter_to_result(meter, single=forecast)

            else:
                return (
                    meter_to_result(meter, single=forecast),
                    meter_to_historical(meter, single=forecast)
                )

        # handle the case for multiple metrics per service (multivalue)
        elif meter.label_dfs is not None:

            # dictionary to associate labels with their forecasted dataframes
            multiple: dict[frozenset[LabelKeyValue], pd.DataFrame] = {}

            future = None

            # iterate over each label dataframe, fit Prophet model, and predict
            for labels, df in meter.label_dfs.items():
                multiple[labels] = forecast_single_value(df)

            # logger.info(f"Predicted for {meter.service_name} of {self.name} to {future["ds"].max()}")

            if not INCLUDE_HISTORICAL:
                return meter_to_result(meter, multiple=multiple)

            else:
                return (
                    meter_to_result(meter, multiple=multiple),
                    meter_to_historical(meter, multiple=multiple)
                )


    def calc_future_period(self, df: pd.DataFrame) -> int:
        df_max_time = pd.to_datetime(df['ds'].max())
        future_dates = pd.date_range(start=df_max_time, end=self.future_max_time, freq=self.conf.frequency)
        if len(future_dates) <= 0:
            return self.conf.period
        return len(future_dates)


    def split_to_meter(self, data: FetchedData):
        
        # handle the case for a single metric per service (singlevalue)
        # <service>     <timestamp>     <value>
        if data.single is not None:

            # separate the data for each service into a container of (service_name, group_df)
            service_df = data.df.groupby(data.single.service_name_column)

            # generate column name map for Prophet
            saved_column = {
                data.single.timestamp_column: 'ds',
                data.single.value_column: 'y'
            }

            # iterate over each (service_name, group_df), shadows original service_df
            for service_name, service_df in service_df:

                # select all rows and columns in saved_column.keys(), then renames them via the map
                renamed_df = service_df.loc[:, saved_column.keys()].rename(columns=saved_column)
                
                # convert datetimes to pandas datetime64 objects 
                renamed_df['ds'] = pd.to_datetime(renamed_df['ds'], format=data.single.time_format)
                
                # remove invalid rows
                renamed_df = renamed_df.dropna()

                # skip analysis if we do not have sufficient data, as defined by self.conf.min_days
                if len(renamed_df) < self.conf.min_days * 24:  # min hours = min_days * 24 hour
                    logger.info(f"Skipping {service_name}({self.name}), less than {self.conf.min_days} "
                                f"days(needs {self.conf.min_days * 24} count) data points(current: {len(renamed_df)})")
                    continue

                # yield per-service
                yield ReadyPredictMeter(service_name, renamed_df)

        # handle the case for multiple metrics per service (multivalue)
        # <service>     <timestamp>     <value1>    <value2>    <value3>    ...
        elif data.multiple is not None:

            # separate the data for each service into a container of (service_name, group_df)
            services_grouped = data.df.groupby(data.multiple.service_name_column)

            # iterate over each (service_name, group_df), shadows original service_df
            for service_name, service_df in services_grouped:

                # initialize a dict that maps sets of LabelKeyValues to dataframes
                service_label_df: dict[frozenset[LabelKeyValue], pd.DataFrame] = {}

                # for each label, perform the same procedure as the single-label variant and associate the label with our df in the map
                # each label is a group of keys and associated values
                for val_conf in data.multiple.value_columns:

                    # filter the service_df to only include the columns we care about
                    saved_column = {
                        data.multiple.time_stamp_column: 'ds',
                        val_conf.value: 'y'
                    }

                    # select all rows and columns in saved_column.keys(), then renames them via the map
                    renamed_df = service_df.loc[:, saved_column.keys()].rename(columns=saved_column)
                    
                    # convert datetimes to pandas datetime64 objects
                    renamed_df['ds'] = pd.to_datetime(renamed_df['ds'], format=data.multiple.time_format)
                    
                    # remove invalid rows
                    renamed_df = renamed_df.dropna()
                    
                    # skip analysis if we do not have sufficient data, as defined by self.conf.min_days
                    if len(renamed_df) < self.conf.min_days * 24:  # min hours = min_days * 24 hour
                        logger.warning("Skipping %s(%s), labels: %s, less than %d data points(current: %d)" %
                            (service_name, self.name, val_conf.tags, self.conf.min_days, len(renamed_df)))
                        continue
                    
                    # associate our dataframe with the label
                    service_label_df[frozenset(val_conf.tags)] = renamed_df

                if len(service_label_df) == 0:
                    logger.warning("Skipping %s(%s), no valid (labels) data points" % (service_name, self.name))
                    continue
                
                # yield per-service
                yield ReadyPredictMeter(service_name, label_dfs=service_label_df)


def calc_max_predict_time(conf: PredictConfig, start_time : datetime.datetime) -> datetime.datetime:
    freq = conf.frequency.lower()

    params={}

    # add layer of indirection because we may want to modify the offset in the future
    offset_period = conf.period

    if freq == 'd':
        params['days'] = offset_period
    elif freq == 'h':
        params['hours'] = offset_period
    elif freq == 't' or freq == 'm' or freq == 'min':
        params['minutes'] = offset_period
    elif freq == 's':
        params['seconds'] = offset_period
    elif freq == 'w':
        params['weeks'] = offset_period
    else:
        raise ValueError(f"Unknown frequency type: {conf.frequency}")

    return start_time + datetime.timedelta(**params)