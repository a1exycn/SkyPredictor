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

import json
import logging
import os
from abc import abstractmethod, ABC
from collections import defaultdict
from typing import NamedTuple
from enum import Enum

import pandas as pd
import requests

from baseline.predict import PredictMeterResult, PredictValue, PredictTimestampWithSingleValue, PredictLabeledWithLabeledValue, LabelKeyValue

# victoria metric
# prometheus

import sqlite3
import peewee


log = logging.getLogger(__name__)


class QueryTimeBucketStep(Enum):
    HOUR = 1


class ResultManager(ABC):

    @abstractmethod
    def save(self, meter_name: str, results: list[PredictMeterResult]):
        pass

    @abstractmethod
    def query(
        self, service_name: str, 
        metrics_names: list[str], 
        start_bucket: int, 
        end_bucket: int,
        step: QueryTimeBucketStep
    ) -> dict[str, list[PredictMeterResult]]:
        pass


class MeterNameResultManager(ResultManager):

    def __init__(self, dir: str):
        self.dir = dir

    def save(self, meter_name: str, results: list[PredictMeterResult]):
        '''
        Save results to a file named after the meter_name in the specified directory.
        If the directory does not exist, it will be created.
        '''
        grouped_results = defaultdict(dict)
        for result in results:
            grouped_results[result.service_name] = result
        file_name = f"{self.dir}/{meter_name}.json"
        os.makedirs(os.path.dirname(file_name), exist_ok=True)
        with open(file_name, 'w', encoding='utf-8') as f:
            json.dump(grouped_results, f, ensure_ascii=False, separators=(',', ':'), cls=ResultEncoder)

    def query(
        self, 
        service_name: str, 
        metrics_names: list[str], 
        start_bucket: int, 
        end_bucket: int,
        step: QueryTimeBucketStep
    ) -> dict[str, list[PredictMeterResult]]:
        '''
        Query results for a specific service and metrics names within the specified time buckets.
        The results are filtered by the service_name and the time range defined by start_bucket and end
        '''
        results: dict[str, list[PredictMeterResult]] = {}
        startTs, endTs = time_bucket_to_timestamp(start_bucket, step), time_bucket_to_timestamp(end_bucket, step)
        for meter_name in metrics_names:
            file_name = f"{self.dir}/{meter_name}.json"
            if os.path.exists(file_name):
                with open(file_name, 'r', encoding='utf-8') as f:
                    try:
                        file_content = f.read()
                        data = json.loads(file_content, object_hook=json_load_object_hook)
                        if service_name in data:
                            service_results = PredictMeterResult.from_dict(data[service_name])
                            service_results.filter_time(startTs, endTs)
                            if meter_name not in results:
                                results[meter_name] = []
                            results[meter_name].append(service_results)
                    except json.JSONDecodeError as e:
                        log.error(f"parsing baseline result file failure, filepath: {file_name}, error: {e}")
            else:
                log.info(f"cannot found the baseline result file: filepath: {file_name}")
        return results


def time_bucket_to_timestamp(bucket: int, step: QueryTimeBucketStep) -> pd.Timestamp:
    if step == QueryTimeBucketStep.HOUR:
        return pd.to_datetime(f"{bucket}", format="%Y%m%d%H")
    log.warning(f"detect the time bucket query is not hour, baseline is not support for now, current: {step}")
    return pd.Timestamp(0)


class ResultEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, frozenset):
            return list(obj)
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        if hasattr(obj, '__dict__'):
            return obj.__dict__
        return super().default(obj)


def json_load_object_hook(dct):
    if 'timestamp' in dct:
        dct['timestamp'] = pd.to_datetime(dct['timestamp'])
    return dct


class Victoria_Metrics_Result_Manager(ResultManager):
    '''
    ResultManager implementation that uses VictoriaMetrics as the backend database.
    '''

    def __init__(self, url_read: str, url_write: str, file_write: str = None):
        self.url_read = url_read
        self.url_write = url_write
        self.file_write = file_write


    class Prefix(Enum):
        LABEL = '@'
        FSET = '$'

        @classmethod
        def is_label(cls, key: str) -> bool:
            '''
            Check if the key is a label.
            This is used to distinguish between labels and other keys in the metric.
            '''
            return key.startswith(cls.LABEL.value)

    
    class Label:
        '''
        Class to define a label in the metric.
        This is used to distinguish between different labels in the metric.
        '''

        @classmethod
        @abstractmethod
        def get_name(cls) -> str:
            pass
    
        @classmethod
        def field_name(cls) -> str:
            return f"{Victoria_Metrics_Result_Manager.Prefix.LABEL.value}{cls.get_name()}"


    class Value_Type(Label, Enum):
        '''
        Enum to define the type of value in the metric.
        This is used to distinguish between lower bound, actual value, and upper bound.
        '''

        LOWER = "lower_bound"
        ACTUAL = "actual_value"
        UPPER = "upper_bound"
        
        @classmethod
        def get_name(cls) -> str:
            return "value_type"


    class Metric_Type(Label, Enum):
        '''
        Enum to define the type of metric.
        This is used to distinguish between single value metrics and labeled metrics.
        '''

        SINGLE = "single"
        LABELED = "labeled"
        
        @classmethod
        def get_name(cls) -> str:
            return "metric_type"


    @classmethod
    def form_metrics(
        cls,
        name : str,
        labels : dict[str, str], 
        values : [PredictValue], 
        timestamps : [int], # in milliseconds since epoch
        service_name : str,
        is_single_metric: bool
    ) -> tuple(dict[str, any]):
        '''
        Form the metrics in the VictoriaMetrics format.
        This method returns a tuple of three dictionaries, ordered as follows:
        1. Lower bound values   
        2. Actual values
        3. Upper bound values
        They are distinct in metric data via a "type" label.
        '''

        labels = {
            f"{cls.Prefix.FSET.value}{key}": value for key, value in labels.items()
        }

        # define helper function to generate the metric form along with the "type" label
        def gen_form(values: [int], type : cls.Value_Type):
            return {
                "metric": {
                    "__name__": name,
                    f"{cls.Prefix.LABEL.value}service_name": service_name,
                    cls.Value_Type.field_name(): type.value,
                    cls.Metric_Type.field_name(): 
                        cls.Metric_Type.SINGLE.value if is_single_metric 
                        else cls.Metric_Type.LABELED.value,
                    **labels
                },
                "values": values,
                "timestamps": timestamps
            }

        
        return (
            gen_form([v.lower_value for v in values], cls.Value_Type.LOWER),
            gen_form([v.value for v in values], cls.Value_Type.ACTUAL),
            gen_form([v.upper_value for v in values], cls.Value_Type.UPPER)
        )


    @classmethod 
    def get_ms(cls, timestamp: pd.Timestamp) -> int:
        '''
        Convert a pandas Timestamp to milliseconds since epoch.
        '''
        return int(timestamp.timestamp() * 1000)  # convert to milliseconds


    def _send_to_victoria_metrics(
        self,
        service_to_array_exports: dict[str, list[dict[str, any]]],
    ):
        '''
        Send the metrics to VictoriaMetrics.
        '''
        url = f"{self.url_write}/api/v1/import"
        headers = {"Content-Type": "application/json"}

        # TODO: if performance suffers, split the data into smaller chunks

        # VictoriaMetrics expects one JSON object per line (JSONL)
        data = ""

        for service_name, metrics in service_to_array_exports.items():
            for metric in metrics:
                data += json.dumps(metric) + "\n"

        # send the data to VictoriaMetrics
        try:
            response = requests.post(url, headers=headers, data=data.encode('utf-8'))
            response.raise_for_status()  # raise an error for bad responses
            log.info(f"Successfully sent metrics to VictoriaMetrics: {response.status_code}")
        except requests.RequestException as e:
            log.error(f"Failed to send metrics to VictoriaMetrics: {e}")
            raise


    def save(self, meter_name: str, results: list[PredictMeterResult]):
        service_to_array_exports : dict[str, list[dict[str, any]]] = {}

        for result in results:

            # handle single value results
            if result.single is not None:

                find = service_to_array_exports.get(result.service_name, None)
                if find is None or result.historical:
                    arr = service_to_array_exports[result.service_name] = []

                    arr.extend(
                        Victoria_Metrics_Result_Manager.form_metrics(
                            name=meter_name,
                            labels={
                                **({'historical': 'true'} if result.historical else {})
                            },
                            values=[t.value for t in result.single],
                            timestamps=[Victoria_Metrics_Result_Manager.get_ms(t.timestamp) for t in result.single],
                            service_name=result.service_name,
                            is_single_metric=True
                        )
                    )

                else:
                    raise ValueError(
                        f"Service {result.service_name} already has a single value metric for {meter_name}"
                    )               

            # handle labeled value results
            elif result.labeled is not None:

                arr = None
                
                # ensure we have a reference to the service_name in the service_to_array_exports
                # dictionary, if not then create it
                if result.service_name not in service_to_array_exports:
                    arr = service_to_array_exports[result.service_name] = []
                else:
                    arr = service_to_array_exports[result.service_name]

                # iterate over each labeled submetric and convert it to the VictoriaMetrics metric format
                for group in result.labeled:
                    unrolled_fset = list(group.label)[0]
                    flattened_fset = {unrolled_fset.key: unrolled_fset.value}

                    arr.extend(
                        Victoria_Metrics_Result_Manager.form_metrics(
                            name=meter_name,
                            labels={
                                **flattened_fset,  # Unpack the flattened set as labels
                                **({'historical': 'true'} if result.historical else {})
                            },
                            values=[
                                t.value
                                for t in group.time_with_values
                            ],
                            timestamps=[
                                Victoria_Metrics_Result_Manager.get_ms(t.timestamp) 
                                for t in group.time_with_values
                            ],
                            service_name=result.service_name,
                            is_single_metric=False
                        )
                    )

            else:
                raise ValueError("Result must have either single or labeled values")
        # end for result in results

        # send the metrics to VictoriaMetrics
        self._send_to_victoria_metrics(service_to_array_exports)
        # self._test_write(service_to_array_exports)


    def query(
        self, 
        service_name: str, 
        metrics_names: list[str], 
        start_bucket: int, 
        end_bucket: int,
        step: QueryTimeBucketStep
    ) -> dict[str, list[PredictMeterResult]]:
        '''
        Query results for a specific service and metrics names within the specified time buckets.
        The results are filtered by the service_name and the time range defined by start_bucket and end_bucket.
        '''

        results: dict[str, list[PredictMeterResult]] = defaultdict(list)
        startTs, endTs = time_bucket_to_timestamp(start_bucket, step), time_bucket_to_timestamp(end_bucket, step)

        url = f"{self.url_read}/api/v1/export"

        for metric_name in metrics_names:
            # Prepare the query parameters

            matchstring = "{__name__=\"" + metric_name + "\", \\@service_name=\"" + service_name + "\"}"

            params = {
                'match[]': matchstring,
                "start": f"{self.get_ms(startTs)}",
                "end": f"{self.get_ms(endTs)}",
            }

            try:
                response = requests.get(url, params=params)
                response.raise_for_status()  # Raise an error for bad responses

                data = response.text.strip().splitlines()
                if not data:
                    log.info(f"No data found for metric {metric_name} for service {service_name} in the specified range.")
                    continue

                # determine if the metric is single or labeled based on the response format and reconstruct the results accordingly
                
                # separately obtain the first request to determine the metric type
                collection_req = [json.loads(line) for line in data]
                req_first = collection_req[0]

                # get the metric type from the first request
                metric_type_first = req_first['metric'][self.Metric_Type.field_name()]


                class Bundled_Values(NamedTuple):
                    '''
                    Class to hold the bundled values for lower, actual, and upper bounds.
                    This is used to reconstruct the PredictTimestampWithSingleValue objects.
                    '''

                    lower_bound: dict
                    actual: dict
                    upper_bound: dict
                # end class Bundled_Values


                def find_value_fields(list : [dict]) -> Bundled_Values:
                    '''
                    Find the lower bound, actual value, and upper bound fields in the list of metrics.
                    '''

                    req_lbound = None
                    req_actual = None
                    req_ubound = None

                    for req in list:
                        value_type = req['metric'][self.Value_Type.field_name()]
                        if value_type == self.Value_Type.LOWER.value:
                            req_lbound = req
                        elif value_type == self.Value_Type.ACTUAL.value:
                            req_actual = req
                        elif value_type == self.Value_Type.UPPER.value:
                            req_ubound = req
                        else:
                            log.warning(f"Unknown value type for {metric_name} for service {service_name}. "
                                "Expected 'lower_bound', 'actual_value', or 'upper_bound', got: "
                                f"{value_type}")
                    
                    if req_lbound is None or req_actual is None or req_ubound is None:
                        raise ValueError(f"Missing one of the required value types for {metric_name} for service {service_name}")

                    return Bundled_Values(
                        lower_bound=req_lbound, 
                        actual=req_actual, 
                        upper_bound=req_ubound
                    )
                # end def find_value_fields


                def proc_group(
                    req_lbound : dict, 
                    req_actual : dict, 
                    req_ubound : dict
                ) -> list[PredictTimestampWithSingleValue]:
                    '''
                    Process a set of 3 associated values (lbound, value, ubound) and reconstruct the list of PredictTimestampWithSingleValue.
                    '''

                    result : list[PredictTimestampWithSingleValue] = []

                    for v_l, v_a, v_u, t_l, t_a, t_u in zip(req_lbound['values'], 
                        req_actual['values'], 
                        req_ubound['values'], 
                        req_lbound['timestamps'],
                        req_actual['timestamps'],
                        req_ubound['timestamps']
                    ):
                        # zip the values and timestamps together
                        # this will create a list of tuples (lower, actual, upper, timestamp)
                        if t_l != t_a or t_a != t_u:
                            raise ValueError(
                                f"Timestamps do not match for {metric_name} for service {service_name}. "
                                "Expected all timestamps to be the same, got: "
                                f"{t_l}, {t_a}, {t_u}"
                            )

                        result.append(
                            PredictTimestampWithSingleValue(
                                value=PredictValue(
                                    lower_value=v_l,
                                    value=v_a,
                                    upper_value=v_u
                                ),
                                timestamp=pd.Timestamp(t_a, unit='ms')
                            )
                        )

                    return result
                # end def proc_group

                # still within per-metric scope
                
                if metric_type_first == self.Metric_Type.SINGLE.value:
                    # This is a single value metric, we need to handle it accordingly
                    # reconstruct the single value metric according to the lower, actual, and upper bound metrics
                    bundle = find_value_fields(collection_req)

                    values = proc_group(
                        req_lbound=bundle.lower_bound,
                        req_actual=bundle.actual,
                        req_ubound=bundle.upper_bound
                    )

                    results[metric_name].append(
                        PredictMeterResult(
                            service_name=service_name,
                            single=values,
                            labeled=None  # No labeled metrics for single value metrics
                        )
                    )
                # end if metric_type_first == self.Metric_Type.SINGLE.value

                elif metric_type_first == self.Metric_Type.LABELED.value:
                    # This is a labeled metric, we need to handle it accordingly
                    # find all equivalent set-labelled metrics and reconstruct in-group according to the lower, actual, and upper bound metrics


                    def group_by_fset(metrics : [dict]) -> dict[str, list[dict]]:
                        '''
                        Group the list of metrics by a fset of their labels.
                        '''

                        grouped = defaultdict(list)

                        for req in metrics:
                            
                            # collect all fset values
                            fset_values : list[LabelKeyValue] = []

                            labels = req['metric']

                            for label in labels:
                                if label.startswith(self.Prefix.FSET.value):
                                    key = label[1:]  # remove the '$' prefix
                                    value = labels[label]
                                    fset_values.append(LabelKeyValue(key, value))

                            # generate a frozenset from the fset values
                            fset = frozenset(fset_values)

                            grouped[fset].append(req)
                            
                        return grouped
                    # end def group_by_fset


                    res_label : [PredictLabeledWithLabeledValue] = []

                    # collect metrics by fset
                    grouped = group_by_fset(collection_req)

                    # we need to organize each collection by fset into PredictLabeledWithLabeledValue, which will then be collected into a list,
                    # which will then be incorporated into the PredictMeterResult
                    for fset, group in grouped.items():
                        # find the lbound, actual, and ubound values of the fset-labelled submetrics
                        bundle = find_value_fields(group)

                        # combine each individual group into a unified timestamp list
                        values = proc_group(
                            req_lbound=bundle.lower_bound, 
                            req_actual=bundle.actual, 
                            req_ubound=bundle.upper_bound
                        )

                        res_label.append(
                            PredictLabeledWithLabeledValue(
                                label=fset,
                                time_with_values=values
                            )
                        )
                    # end for fset, group in grouped.items()

                    results[metric_name].append(
                        PredictMeterResult(
                            service_name=service_name,
                            single=None,  # No single value metrics for labeled metrics
                            labeled=res_label
                        )
                    )
                # end elif metric_type_first == self.Metric_Type.LABELED.value

                else:
                    raise ValueError(
                        f"Unknown metric type for {metric_name} for service {service_name}. "
                        "Expected 'single' or 'labeled', got: "
                        f"{metric_type_first}"
                    )
                    continue
                # end else
            # end try

            except requests.RequestException as e:
                log.error(f"Failed to query VictoriaMetrics: {e}")
                raise

            # end for metric_name in metrics_names

        return results
    # end def query
# end class Victoria_Metrics_Result_Manager