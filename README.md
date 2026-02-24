This repo is the study case of Carlos Sandoval Appliance to BMST team.

Tableau direction of results: https://us-east-1.online.tableau.com/#/site/quezandoc-72012affd3/workbooks/4095663/views

## How the code works

You need to make a virtual enviroment with
```
python3 -m venv .v
source .v/bin/activate
```
Activating it and installing libraries related to the proyect with
```
pip install -r r.txt
```
For generating the output you should run `python app.py`.

## Code details

- `experimenting.py` is a code flow that i'd use for reconozing data.
- `app.py` is the main flow of the generated data views for Tableau.
- `query_funcions.py` is the core of the preparation of the view data for every KPI behind there are a function here.
- `duck.py` is the in memory DBM for making querys to data.
- `logging_decorator.py` is the formating of the output logs.
- `data_types.py` is the definition of the type of every column header.

## KPI definitions

The analytical core of this project focuses on three operational issues: Anomaly Detection, Hardware Reliability, and Fleet Connectivity.

1. Sensor Anomaly & Performance

    Measures the health and safety of individual sensor using a hybrid detection strategy.

    - Z-Score (Statistical Deviation): Quantifies how many standard deviations a metric (Temperature/Pressure) deviates from its 7-day rolling average. Values where ∣z∣>3 are flagged as anomalies.

    - ML Anomaly Score: Generated via Isolation Forest, this identifies multivariate outliers and complex behavioral patterns (e.g., "Physics Mismatches" like high temperature paired with low pressure) that traditional thresholds might miss.

    - Global Anomaly Flag: A consolidated binary indicator that triggers when statistical or ML-based detection identifies a risk.

2. Sensor Health & Availability

    Evaluates the coverage and data integrity of the sensor transmitting rate.
    Measure the Sensor Availability % evaluating if they transmitting to the hub.

3. Device (Hub) Connectivity

    Monitors the performance of the telematics gateways (Concentrators) installed on the vehicles.

    - Connectivity Efficiency % (Uptime): The ratio of "Talk Time" vs. "Silence Time" This KPI identifies hardware power failures.

    - Operational Fleet Capacity: A categorical distribution of the fleet status (Operational, In Maintenance, or Out of Service).

## Key findings

1. The device transmition rate isn't related to sensor transmition rate.
2. The sums of duration times of a sensor in levels isn't related to sensor transmition rate.
3. Every reports is for day and the transmitting duration is related to the seconds of the day that they are 86400.
4. Some vehicles doesn't have a device/hub meanwhile some others have 1 or 2.

## Assumptions

1. I assume that the priority of a sensor is just for sending methods.
2. I assume that the sensor will send the report with or without device on board from near devices.
3. I assume that presure will be related to the temparature.
4. I assume that cold pressure is when you inflates the wheel and hot pressure is when the truck is on use.

## Limitations

1. The Issolation forest randomness with the apttern recognition, I made some pattern recognition through Z-score fore give more information but sometimes an anomaly detected by ML is inside of the black box.
2. Z-Score with 7 days rolling window will have issues with less than a week of data.
3. I can't relate the sensor transmition rate with the device transmition rate
4. The anomaly detection is unsupervised, without a ground truth, so i can't validate it with a confusion matrix for FP.