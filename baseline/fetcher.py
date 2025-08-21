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
from abc import ABC, abstractmethod
import datetime
from typing import Optional
import os

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

from baseline.config.config import BaselineFetchConfig

OAP_LOCAL = os.getenv("OAP_LOCAL", "false").lower() in ('true', '1', 't')

logger = logging.getLogger(__name__)

max_fetch_data_period = 80


class LabelKeyValue:
    '''
    represent a k-v pair for a metric label
    '''

    key: str
    value: str

    def __init__(self, key: str, value: str):
        self.key = key
        self.value = value

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "%s=%s" % (self.key, self.value)

    @staticmethod
    def from_dict(d: dict) -> "LabelKeyValue":
        return LabelKeyValue(d["key"], d["value"])

    def __eq__(self, other):
        if not isinstance(other, LabelKeyValue):
            return False
        return self.key == other.key and self.value == other.value

    def __hash__(self):
        return hash((self.key, self.value))


class FetchedSingleDataConfig:
    '''
    describe the structure of a dataframe for a single metric per service
    '''

    service_name_column: str
    timestamp_column: str
    value_column: str
    time_format: str

    def __init__(
        self, 
        service_name_column: str, 
        timestamp_column: str, 
        value_column: str, 
        time_format: str
    ):
        self.service_name_column = service_name_column
        self.timestamp_column = timestamp_column
        self.value_column = value_column
        self.time_format = time_format


class FetchedMultipleValueColumnConfig:
    '''
    describe the structure of a dataframe for one metric out of multiple metrics per service
    '''

    tags: list[LabelKeyValue]
    value: str

    def __init__(self, tags: list[LabelKeyValue], value: str):
        self.tags = tags
        self.value = value


class FetchedMultipleDataConfig:
    '''
    describe the structure of a dataframe for multiple metrics per service
    '''

    service_name_column: str
    time_stamp_column: str
    value_columns: list[FetchedMultipleValueColumnConfig]
    time_format: str

    def __init__(
        self, 
        service_name_column: str, 
        time_stamp_column: str, 
        value_columns: list[FetchedMultipleValueColumnConfig], 
        time_format: str
    ):
        self.service_name_column = service_name_column
        self.time_stamp_column = time_stamp_column
        self.value_columns = value_columns
        self.time_format = time_format


class FetchedData:
    '''
    desctibe the data fetched from the OAP server
    '''

    def __init__(self, df: pd.DataFrame, single: FetchedSingleDataConfig, multiple: FetchedMultipleDataConfig):
        self.df = df
        self.single = single
        self.multiple = multiple


class Fetcher(ABC):
    '''
    Abstract base class for fetching metrics data from a source.
    '''

    @abstractmethod
    def metric_names(self) -> list[str]:
        """
        Return a list of metric names that this fetcher can provide.

        Returns:
            list[str]: The names of the metrics available for fetching.
        """

        pass

    def ready_fetch(self, start_time : datetime.datetime):
        """
        Prepare the fetcher for data retrieval.

        This method can be overridden to perform any setup required before fetching,
        such as initializing connections or loading configuration. By default, it does nothing.
        """

        pass

    @abstractmethod
    def fetch(self, metric_name: str, end_time : datetime.datetime) -> Optional[FetchedData]:
        """
        Fetch data for the specified metric.

        Args:
            metric_name (str): The name of the metric to fetch.

        Returns:
            Optional[FetchedData]: The fetched data for the metric, or None if not available.
        """

        pass


class GraphQLFetcher(Fetcher):
    '''
    Fetcher implementation that retrieves metrics data from a GraphQL API.
    '''

    def __init__(self, conf: BaselineFetchConfig):
        self.conf = conf
        self.services = None
        self.total_period = None
        self.metrics = conf.metrics
        self.base_address = conf.server.address if not conf.server.address.endswith("/") else conf.server.address[:-1]


    def metric_names(self) -> list[str]:
        return self.conf.metrics


    def ready_fetch(self, end_time : datetime.datetime):
        '''
        Prepare the fetcher for data retrieval by fetching all services and calculating the total period needed for queries.
        '''

        all_services = set()

        for layer in self.conf.server.layers:
            
            # fetch all services in the layer
            services = self.fetch_layer_services(layer, end_time)

            for service in services:
                all_services.add(service)

        self.services = all_services
        self.total_period = self.query_need_period()


    def fetch(
        self, 
        metric_name : str, 
        end_time : datetime.datetime
    ) -> Optional[FetchedData]:
        '''

        '''

        # skip retrieval if no services are available
        if self.services is None or len(self.services) == 0:
            return None

        fetch_data = None

        # iterate across all services and fetch metric data for each
        for (service, normal) in self.services:
            fetch_data = self.fetch_service_metrics(service, normal, metric_name, fetch_data, end_time)

        return fetch_data


    def fetch_service_metrics(
        self, 
        service_name : str, 
        normal : bool, 
        metric_name : str, 
        prev_data : FetchedData,
        end_time : datetime.datetime
    ) -> FetchedData:
        count = 0
        for start, end in self.generator_generate_time_buckets(end_time):
            prev_data, per_count = self.fetch_service_metrics_with_rangs(service_name, normal, metric_name, prev_data, start, end)
            count += per_count
        logger.info(f"Total fetched {count} data points for {metric_name}(service: {service_name})")
        return prev_data


    def fetch_service_metrics_with_rangs(
        self, 
        service_name: str, 
        normal: bool, 
        metric_name: str,
        prev_data: FetchedData, 
        start: str, 
        end: str
    ) -> tuple[FetchedData, int]:

        # define the GraphQL query payload

        if OAP_LOCAL:
            payload = {
                "query": """
                    query MetricsQuery($duration: Duration!) {
                        result: execExpression(
                            expression: "view_as_seq(%s)\"
                            entity: {
                                serviceName: "%s"
                                normal: %s
                            }
                            duration: $duration
                        ) {
                            results {
                                metric {
                                    labels { key value }
                                }
                                values { id value }
                            }
                        }
                    }
                """ % (metric_name, service_name, str(normal).lower()),
                "variables": {
                    "duration": {
                        "start": start,
                        "end": end,
                        "step": self.conf.server.down_sampling,
                    }
                }
            }

        else:
            payload = {
                "query": """
                    query queryData($condition: MetricsCondition!, $duration: Duration!) {
                        readMetricsValues: readMetricsValues(condition: $condition, duration: $duration) {
                            label
                            values {
                                values {value}
                            }
                        }
                    }
                """,
                "variables": {
                    "duration": {
                        "start": start,
                        "end": end,
                        "step": self.conf.server.down_sampling,
                    },
                    "condition": {
                        "name": metric_name,
                        "entity": {
                            "scope": "Service",
                            "serviceName": service_name,
                            "normal": normal,
                            "destNormal": False
                        }
                    }
                }
            }

        logger.debug(f"payload is\n{payload}")

        # fetch data from the GraphQL API
        if OAP_LOCAL:
            fetched = self.fetch_data(f"{self.base_address}/graphql", payload)

        else:
            fetched = self.augmented_fetch_data(f"{self.base_address}/graphql", payload, start, end)

        results = fetched['result']['results']

        if len(results) == 0:
            logger.debug(f"No data found for {metric_name}(service: {service_name}) from {start} to {end}")
            return prev_data, 0
        logger.debug(f"Fetch {len(results)} data points for {metric_name}(service: {service_name}) from {start} to {end}")

        # check to see if this is the first time fetching data for this metric - if so, initialize the dataframe
        if prev_data is None:

            df = pd.DataFrame()
            single, multiple = None, None

            if len(results) == 1 and len(results[0]['metric']['labels']) == 0:
                single = FetchedSingleDataConfig(
                    service_name_column="svc",
                    timestamp_column="ts",
                    value_column="value",
                    time_format=self.query_metric_time_format()
                )

            else:
                value_columns = []

                for inx, result in enumerate(results):
                    value_columns.append(
                        FetchedMultipleValueColumnConfig(
                            tags=[
                                LabelKeyValue(label['key'], label['value']) 
                                for label in result['metric']['labels']
                            ],
                            value="label_%d" % inx
                        )
                    )

                multiple = FetchedMultipleDataConfig(
                    service_name_column="svc",
                    time_stamp_column="ts",
                    value_columns=value_columns,
                    time_format=self.query_metric_time_format()
                )

            prev_data = FetchedData(df, single, multiple)
        # end if prev_data is None

        min_date = None
        max_date = None
        count = 0

        # handle the case where we have a single metric
        if prev_data.single is not None:

            df = prev_data.df
        
            rows = []

            for result in results:

                for val in result['values']:
                    v = val.get('value')

                    if v is None:
                        continue

                    max_date = self.convert_metric_time(int(val['id']))

                    if min_date is None:
                        min_date = max_date

                    rows.append(
                        {
                            prev_data.single.service_name_column: service_name,
                            prev_data.single.timestamp_column: max_date,
                            prev_data.single.value_column: int(v)
                        }
                    )
            
            count = len(rows)
            if rows:
                df = pd.concat([df, pd.DataFrame.from_records(rows)], ignore_index=True)

            prev_data.df = df
            logger.info(f"Fetched {count} data points for {metric_name}(service: {service_name}) from {min_date} to {max_date}, "
                        f"original query time range({self.conf.server.down_sampling}): {start} to {end}")
            
            return prev_data, count

        elif prev_data.multiple is not None:

            df = prev_data.df

            rows = []

            for val_inx, values in enumerate(results[0]['values']):

                cur_date = self.convert_metric_time(int(values['id']))

                row = {
                    prev_data.multiple.service_name_column: service_name,
                    prev_data.multiple.time_stamp_column: max_date
                }

                has_value = False

                for inx, result in enumerate(results):

                    for _ in result['metric']['labels']:

                        val = results[inx]['values'][val_inx]['value']

                        if val is None:
                            continue

                        max_date = cur_date

                        if min_date is None:
                            min_date = max_date

                        row["label_%d" % inx] = int(val)
                        has_value = True

                if has_value:
                    rows.append(row)

            if rows:
                df = pd.concat([df, pd.DataFrame.from_records(rows)], ignore_index=True)
                count = len(rows)

            prev_data.df = df
            logger.info(f"Fetched {count} data points for {metric_name}(service: {service_name}) from {min_date} to {max_date}, "
                        f"original query time range({self.conf.server.down_sampling}): {start} to {end}")

        return prev_data, count


    def fetch_layer_services(self, layer: str, end_time: datetime.datetime) -> list[tuple[str, bool]]:

        if OAP_LOCAL:
            payload = {
                "query": """
                    query queryServices($layer: String!) {
                        services: listServices(layer: $layer) {
                            label: name
                            normal
                        }
                    }
                """,
                "variables": {"layer": layer}
            }
        # end if OAP_LOCAL

        else:
            payload = {
                "query": """
                    query queryServices($duration: Duration!,$keyword: String!) {
                        services: getAllServices(duration: $duration, group: $keyword) {
                            key: id
                            label: name
                            group
                        }
                    }
                """,
                "variables": {
                    "duration": {
                        "start": (end_time - datetime.timedelta(days=self.conf.server.days_of_data)).strftime('%Y-%m-%d %H%M'),
                        "end": end_time.strftime('%Y-%m-%d %H%M'),
                        "step": "MINUTE"
                    },
                    "keyword": ""
                }
            }
        # end else

        services = self.fetch_data(f"{self.base_address}/graphql", payload)['services']

        # # hardcoded test service
        # services = [
        #     {
        #         'key': 'aW5mLW5vdmEtZ3ctc3dlYg==.1',
        #         'label': 'inf-nova-gw-sweb',
        #     }
        # ]

        names = []

        for service in services:
            # TODO: verify the "normality", because in our current OAP data there is no such field
            names.append((service['label'], True))

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Fetch {names} services from layer {layer}")

        return names


    def query_need_period(self) -> int:

        if OAP_LOCAL:
            resp = self.fetch_get_data(f"{self.base_address}/status/config/ttl")
            total_days = int(resp['metrics']['day'])

        else:
            total_days = self.conf.server.days_of_data
    
        sampling = self.conf.server.down_sampling.lower()
        if sampling == 'hour':
            return total_days * 24
        elif sampling == 'minute':
            return total_days * 24 * 60
        raise Exception("Unsupported down sampling: %s" % sampling)


    def generate_time_buckets(self) -> list[tuple[str, str]]:
        """
        WARNING: deprecated, use generator_generate_time_buckets instead.
        """
        raise DeprecationWarning("This method is deprecated, use generator_generate_time_buckets instead.")

        # we use now as the end time, and calculate the start time based on the total period needed
        end_time = datetime.datetime.now()
        sampling = self.conf.server.down_sampling.lower()
        if sampling == 'hour':
            start_time = end_time - datetime.timedelta(hours=self.total_period)
            return self.generate_time_buckets_by_range(start_time, end_time, self.delta_hour, '%Y-%m-%d %H')
        elif sampling == 'minute':
            start_time = end_time - datetime.timedelta(minutes=self.total_period)
            return self.generate_time_buckets_by_range(start_time, end_time, self.delta_minute, '%Y-%m-%d %H%M')
        raise Exception("Unsupported down sampling: %s" % sampling)


    def generate_time_buckets_by_range(self, start, end, delta, formate) -> list[tuple[str, str]]:
        """
        WARNING: deprecated, use generator_generate_time_buckets instead.
        """
        raise DeprecationWarning("This method is deprecated, use generator_generate_time_buckets instead.")

        cur_end_time = start + delta(max_fetch_data_period - 1)
        if cur_end_time > end:
            return [start.strftime(formate), end.strftime(formate)]
        results = []
        while start < cur_end_time < end:
            results.append([start.strftime(formate), cur_end_time.strftime(formate)])
            start = cur_end_time + delta(1)
            cur_end_time += delta(max_fetch_data_period)
        if start < end:
            results.append((start.strftime(formate), end.strftime(formate)))
        return results

    
    def generator_generate_time_buckets_aux(
        self, 
        start, 
        end, 
        delta, 
        format
    ) -> tuple[str, str]:
        '''
        Auxiliary generator function to yield time buckets based on the start and end times, delta, and format.
        '''

        cur_end_time = start + delta(max_fetch_data_period - 1)

        if cur_end_time > end:
            return (start.strftime(format), end.strftime(format))

        while start < cur_end_time < end:
            yield [start.strftime(format), cur_end_time.strftime(format)]
            start = cur_end_time + delta(1)
            cur_end_time += delta(max_fetch_data_period)

        if start < end:
            return (start.strftime(format), end.strftime(format))


    def generator_generate_time_buckets(self, end_time : datetime.datetime) -> tuple[str, str]:
        '''
        Generate time buckets based on the configured down sampling method and the total period needed for queries.
        This method yields tuples of start and end times formatted according to the sampling method.
        The logic is based on generate_time_buckets, but uses a generator to yield results one by one to save memory.
        '''

        sampling = self.conf.server.down_sampling.lower()

        # choose the appropriate time delta and format based on the sampling method
        if sampling == 'hour':
            start_time = end_time - datetime.timedelta(hours=self.total_period)
            delta = self.delta_hour
            format = '%Y-%m-%d %H'

        elif sampling == 'minute':
            start_time = end_time - datetime.timedelta(minutes=self.total_period)
            delta = self.delta_minute
            format = '%Y-%m-%d %H%M'

        else:
            raise Exception("Unsupported down sampling: %s" % sampling)

        # invoke the auxiliary generator function to yield time buckets
        yield from self.generator_generate_time_buckets_aux(start_time, end_time, delta, format)


    def delta_hour(self, val):
        return datetime.timedelta(hours=val)


    def delta_minute(self, val):
        return datetime.timedelta(minutes=val)


    def query_metric_time_format(self) -> str:
        sampling = self.conf.server.down_sampling.lower()
        if sampling == 'hour':
            return '%Y%m%d%H'
        elif sampling == 'minute':
            return '%Y%m%d%H%M'
        raise Exception("Unsupported down sampling: %s" % sampling)


    def convert_metric_time(self, val_id: int) -> str:
        return datetime.datetime.fromtimestamp(val_id / 1000).strftime(self.query_metric_time_format())


    def fetch_get_data(self, url):
        if self.conf.server.username and self.conf.server.password:
            auth = HTTPBasicAuth(self.conf.server.username, self.conf.server.password)
        else:
            auth = None

        response = requests.get(url, auth=auth, headers={"Accept": "application/json"})
        if response.status_code != 200:
            raise Exception("Failed to fetch data from GraphQL: %s" % response.text)
        return response.json()


    def fetch_data(self, address, payload):
        if self.conf.server.username and self.conf.server.password:
            auth = HTTPBasicAuth(self.conf.server.username, self.conf.server.password)
        else:
            auth = None

        response = requests.post(
            address,
            json=payload,
            auth=auth,
            headers={"Content-Type": "application/json"}
        )

        if response.status_code != 200:
            raise Exception("Failed to fetch data from GraphQL: %s" % response.text)
            
        return response.json()['data']

    def augmented_fetch_data(
        self, 
        address : str, 
        payload : dict,
        start : str,
        end : str
    ):
        """
        """
        
        data_pre = None

        try:
            data_pre = self.fetch_data(address, payload)
        except Exception as e:
            logger.error(f"Error fetching data from {address}: {e}")
            return None

        data_aug = {
            'result': {
                'results': [
                    {
                        'metric': {
                            'labels': [],
                        },
                        'values': []
                    }
                ]
            }
        }

        ts_start = datetime.datetime.strptime(start, '%Y-%m-%d %H%M')
        ts_end = datetime.datetime.strptime(end, '%Y-%m-%d %H%M')
        ts_step = self.conf.server.down_sampling

        if ts_step.lower() in ['m', 't', 'min', 'minute']:
            delta = datetime.timedelta(minutes=1)
        elif ts_step.lower() in ['h', 'hour']:
            delta = datetime.timedelta(hours=1)
        elif ts_step.lower() in ['d', 'day']:
            delta = datetime.timedelta(days=1)
        else:
            raise Exception("Unsupported down sampling: %s" % ts_step)

        def generate_ts():
            current = ts_start
            while current <= ts_end:
                # Convert to milliseconds since epoch
                yield int(current.timestamp() * 1000)
                current += delta

        for item, timestamp_id in zip(data_pre['readMetricsValues']['values']['values'], generate_ts()):
            val = item['value']

            if val is None:
                continue

            if int(val) != 0:
                # notify that we found nonzero value
                logger.debug(f"Found nonzero value {val} for metric {payload['variables']['condition']['name']} at timestamp {timestamp_id}")

            data_aug['result']['results'][0]['values'].append(
                {
                    'id': timestamp_id,
                    'value': int(val)
                }
            )

        return data_aug
    # end def augmented_fetch_data
