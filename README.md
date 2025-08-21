# SkyPredictor
SkyPredictor is a metric trend predictor to generate alarm baselines.

## Why use SkyWalking Predictor?

In the SkyWalking alert system, alerts can only be configured using predefined fixed values (thresholds).
However, in real-world scenarios, thresholds often change over time.
For example, the system's CPU usage during the morning is significantly different
from the peak business hours during the day.
This makes dynamic baseline prediction particularly important.

The SkyWalking Predictor service can periodically collect metric data
from the SkyWalking service and predict metric values for a future period.

## Architecture

![Architecture](architecture.png)

- **Predictor**: Fetch data from OAP and provides a gRPC API for querying predicted metric values. *Modifications have been made so that predicated metrics are sent to an instance of a VictoriaMetrics database. Please modify BASELINE_PREDICT_URL_WRITE and BASELINE_PREDICT_URL_READ environment variables.*
- **OAP**: It provides a data query protocol and periodically retrieves predicted data from the Predictor,
  integrating it into the alert module.

## Configuration

It is suggested that you use the provided `run_skypredictor.sh` and follow the provided format to configure the following documented environmental variables to your needs.

### Logging

Configure system logs in SkyWalking Predictor.

| Name           | Default                                               | Environment Key | Description               |
|----------------|-------------------------------------------------------|-----------------|---------------------------|
| logging.level  | INFO                                                  | LOGGING_LEVEL   | Minimum log output level. |
| logging.format | :%(asctime)s - %(name)s - %(levelname)s - %(message)s | LOGGING_FORMAT  | Log output format.        |

### Server

Configure the external services provided by SkyWalking Predictor.

| Name                   | Default | Environment Key | Description                                              |
|------------------------|---------|-----------------|----------------------------------------------------------|
| server.grpc.port       | 18080   | GRPC_PORT       | Port for providing external gRPC services.               |
| server.monitor.enabled | true    | MONITOR_ENABLED | Whether to enable Prometheus metrics monitoring service. |
| server.monitor.port    | 8000    | MONITOR_PORT    | Port for providing external monitoring services.         |

### Baseline

Configure how to fetch data, and prediction metrics for dynamic baseline.

Please make these module in OAP have been activated:
1. **status-query**: Query `/status/config/ttl` for getting TTL of days for fetch all metrics data.
2. **graph** in **query**: Query service, metrics from GraphQL.

| Name                                | Default                        | Environment Key                     | Description                                                  |
| ----------------------------------- | ------------------------------ | ----------------------------------- | ------------------------------------------------------------ |
| baseline.cron                       | */8 * * * *                    | BASELINE_FETCH_CRON                 | Configure the execution timing of data retrieval and prediction for the baseline by a cron expression. |
| baseline.fetch.server.address       | http://localhost:12800/        | BASELINE_FETCH_SERVER_ENDPOINT      | Address of OAP Restful server.                               |
| baseline.fetch.server.username      |                                | BASELINE_FETCH_SERVER_USERNAME      | If OAP access requires authentication, the username must be provided. |
| baseline.fetch.server.password      |                                | BASELINE_FETCH_SERVER_USERNAME      | If OAP access requires authentication, the password must be provided. |
| baseline.fetch.server.down_sampling | HOUR                           | BASELINE_FETCH_SERVER_DOWN_SAMPLING | Specify the type of downsampling data to download from OAP, supporting `HOUR` and `MINUTE`. Note that retrieving minute-level data takes a longer time. |
| baseline.fetch.server.layers        | GENERAL                        | BASELINE_FETCH_SERVER_LAYERS        | Specify which layer service data needs to be fetch. Use a comma(`,`) to separate multiple layers. |
| baseline.fetch.server.days_of_data  | 7                              | BASELINE_FETCH_SERVER_DAYS_OF_DATA  | Specifies up to how many days worth of prior of data we should fetch, relative to the specified cron schedule runtime |
| baseline.fetch.metrics              | service_cpm,service_percentile | BASELINE_FETCH_METRICS              | List of metrics to be monitored. Use a comma(`,`) to separate multiple names. |
| baseline.predict.directory          | ./out_predict                  | BASELINE_PREDICT_DIRECTORY          | The directory for save prediction results for query purposes. |
| baseline.predict.url_write          | None                           | BASELINE_PREDICT_URL_WRITE          | The URL towards the VictoriaMetrics databse write endpoint   |
| baseline.predict.url_read           | None                           | BASELINE_PREDICT_URL_READ           | the URL towards the VictoriaMetrics database read endpoint   |
| baseline.predict.min_days           | 2                              | BASELINE_PREDICT_MIN_DAYS           | The minimum number of days of data required for metric prediction, preventing inaccuracies due to insufficient data. |
| baseline.predict.frequency          | h                              | BASELINE_PREDICT_FREQUENCY          | Specify the frequency of the predicted data. Currently, only hourly (`h`) is supported. |
| baseline.predict.period             | 24                             | BASELINE_PREDICT_PERIOD             | Specify the number of future data points to predict.         |
| baseline.predict.offset             | 0                              | BASELINE_PREDICT_OFFSET             | (NOT YET FUNCTIONAL) Specify the number of days to offset the predicted results from the actual data |

### Others

| `ENVIRONMENT_Variable` | `default` | `purpose`                                                    |
| ---------------------- | --------- | ------------------------------------------------------------ |
| `OAP_LOCAL`            | `False`   | set to `True` if you want the locally provided test OAP fetch format, `False` if you want the fetch format from http://swnova.zlfang.com/graphql |
| `INCLUDE_HISTORICAL`   | `False`   | set to `True` if you want to also send the fetched historical data as-is to VictoriaMetrics, `False` otherwise |

## Deployment

### VM Deployment

Currently, VM deployment only supports single-node deployment.

#### Requirements

Please ensure that Python is installed on the local machine, and version `>= 3.12`.

#### Startup

```shell
# install all dependencies
make install
# starting predictor server
python3 -m server.server
```

### Kubernetes

#### Configure Metrics

Please edit the [config.yaml](examples/configmap.yaml) file to deploy in your Kubernetes cluster.
The following configuration need to be updated:
1. **OAP Restful Address**: The Restful Address of OAP to fetch metrics data.
2. **Metrics List**: Which metrics need to be fetched for prediction.

#### Deploy Predictor

Please follow the [deployment.yml](examples/deployment.yml) to deploy the predictor in your Kubernetes cluster.
Update the comment in the file, which includes two configs:
1. **PVC Storage Size**: The storage size for storage the prediction result.

Then, you could use `kubectl apply -f deployment.yml` to deploy the SkyWalking Predictor into your cluster.

NOTE: The Predictor service currently does not support cluster awareness.
Additionally, due to the `ReadWriteOnce` limitation of PVC, it can only be deployed on a single node.
