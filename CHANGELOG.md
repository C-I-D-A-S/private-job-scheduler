# (2020-06-11)

## 0.0.3 (2020-06-11)

### New

- Send Job to `private-job-trigger`
- Add Queue selection: Zip selector, Top level selector
- Add Queue scheduling: deque, bisect
- Add resources checking for dynamic resource job

### Improvements

- apply queue exceptions
- update log formate

### Fix

- Recalculate schedule_time with utc+0 timezone

## 0.0.2 (2020-05-26)

### Features

- Consume Job from `Kafka`
- Send Job to `Airflow`
- Queue selection: weight random selection
- Job selection: check system job
- queue scheduling: FCFS, Heap
