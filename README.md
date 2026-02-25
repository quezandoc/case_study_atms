This repository contains the case study developed by Carlos Sandoval for the BMST team application.

Tableau dashboards are in this links:
- Global Statistics:  https://us-east-1.online.tableau.com/t/quezandoc-72012affd3/views/bmst_atms_17720419832100/Statistics/7e5a5a4d-86e9-4ca6-8d2b-7bea812a13a4/9a673cff-f409-4ca1-87ba-e4bd2c3dfb92
- Anomaly report: https://us-east-1.online.tableau.com/t/quezandoc-72012affd3/views/bmst_atms_17720419832100/Anomalyreport/5b18f673-c78e-4ace-81ad-6b2ed6cdb6af/64d9044e-b9b0-4e96-ae4f-c3735de76eac
- Anomaly for vehicle overall: https://us-east-1.online.tableau.com/t/quezandoc-72012affd3/views/bmst_atms_17720419832100/Vehicleoveralltimeline/58669336-cd84-49d8-8223-f1f6f7422b67/e2150ecb-5c40-489c-b60a-ec0e1cdc9918
- Anomaly for vehicle sensor: https://us-east-1.online.tableau.com/t/quezandoc-72012affd3/views/bmst_atms_17720419832100/Sensorvehicletimeline/1a717b12-2e42-4417-8bf3-0a8a122a4f61/d52e4e29-7f0b-4537-b03a-0b05352795f7

Explanation video: https://youtu.be/d7Esn5k8aOw

File of teableau present on: `tableau_workbook_atms.twbx`

## How the code works

You need to put the input data on a folder on the root called `data/`
And create another folder called `output/` for the creation of outputs

Set up a virtual environment and install the dependencies:
``` bash
python3 -m venv .v
source .v/bin/activate
pip install -r r.txt
```
To process the data and generate the outputs for Tableau, run:
``` bash
python app.py
```

## Code details

- `experimenting.py` Initial EDA and data exploration workflow.
- `app.py` Main execution script for generating Tableau data views.
- `query_funcions.py` Core logic for KPI calculations and data preparation.
- `duck.py` In-memory database management (DuckDB) for efficient querying.
- `logging_decorator.py` Custom formatting for execution logs.
- `data_types.py` Schema definitions and column type enforcement.

## KPI definitions

The analytical core of this project focuses on three operational issues: Anomaly Detection, Hardware Reliability, and Fleet Connectivity.

1. Sensor Anomaly & Performance

    Measures the health and safety of individual sensor using a hybrid detection strategy.

    - Z-Score (Statistical Deviation): Quantifies how many standard deviations a metric (Temperature/Pressure) deviates from its 7-day rolling average. Values where ∣z∣>3 are flagged as anomalies.

    - ML Anomaly Score: Generated via Isolation Forest, this score identifies multivariate outliers and complex behavioral patterns. The algorithm is particularly well-suited for vehicle sensor data collected over diverse terrains, as it adapts to the specific shape of the data to effectively isolate anomalies.

    - Global Anomaly Flag: A consolidated binary indicator that triggers when statistical or ML-based detection identifies a risk.

2. Sensor Health & Availability

    Evaluates data integrity and transmission rates to determine the Sensor Availability %, verifying if sensors are successfully reporting to the hub.

3. Device (Hub) Connectivity

    Monitors the performance of the telematics gateways (Concentrators) installed on the vehicles.

    - Connectivity Efficiency % (Uptime): The ratio of "Talk Time" vs. "Silence Time" This KPI identifies hardware power failures.

    - Operational Fleet Capacity: A categorical distribution of the fleet status (Operational, In Maintenance, or Out of Service).

## Key findings

1. Decoupled Transmission: Device transmission rates do not directly correlate with individual sensor transmission rates.
2. Level Duration vs. Rate: The accumulated duration of sensor levels is independent of the transmission frequency.
3. Daily Reporting: Reports are processed daily; duration metrics are normalized against the 86,400 seconds in a day.
4. Hardware Variance: Fleet configuration is inconsistent; vehicles may have 0, 1, or 2 hubs/devices.

## Assumptions

1. Priority Logic: Sensor priority is assumed to only dictate transmission protocols/methods.
2. Mesh Reporting: Sensors may transmit data via nearby devices if the assigned on-board hub is missing or inactive.
3. Correlation: A physical correlation is assumed between pressure and temperature trends.
4. Thermal States: Distinct thresholds are assumed for "Cold Pressure" (static) and "Hot Pressure" (active operation).

## Limitations

1. Model Interpretability (Black Box): While Isolation Forest is effective at detecting complex patterns, its stochastic nature can make specific detections difficult to interpret. I implemented Z-Score recognition to provide statistical context, but some ML-flagged anomalies remain within the "black box" of the model's internal partitions.
2. Cold Start Problem: The 7-day rolling Z-Score requires a full week of historical data to reach optimal accuracy.
3. Transmission Correlation: Current data does not allow for a direct relational link between sensor-level and device-level transmission health.
4. Unsupervised Validation: Since the detection is unsupervised and lacks "ground truth" labels, it is not possible to calculate a Confusion Matrix or precisely quantify False Positives (FP).