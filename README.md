# job-scheduler

A component to arrange the order data analysis job

## Usage

### Msg Queue Schema

#### Msg Key

UUIDv4 e.g. 1b16f76f-4bf0-44e1-9140-4a91c2e4e3ac

#### Msg Value

|Name|Type|Description|
|--|--|--|
|path|string|path of data analysis api|
|username|string|the caller of this request|
|job_id|string| the id of this job|
|job_config|dict|common config of analysis job. e.f. request_time, deadline|
|job_parameters|dict| the job related parameters|

example

```lan=json
{

    'username': 'ncku_r',
    'job_type': 'real-demand',
    'job_id': '1b16f76f-4bf0-44ew-9140-4a91c2e4e3ab',
    'job_config': {
        'request_time': '2019-12-29 14:00:00'
        'deadline': '2019-12-29 14:30:00',
    }
    'job_parameters': {
        'begin_time': '2019-12-29 14:00:00',
        'end_time': '2019-12-29 17:00:00',
        'value': 1.5,
        'gateway_id': 'X1234567812345678'
    }
}
```
