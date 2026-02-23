### ONLY SCHEMA AND RAW DATA
Here we have 2 sets of data `time_in_level_device` and `time_in_level_sensor`. In both we've a *relation to a **vehicle with his id***, so the main relation from both tables is this.

As i can see the `device is 1:1 to the vehicle` and has *transmission rates and a field that informs when the report starts*.

On the other hand we have sensors that `1 vehicle could has many sensors on different wheels` and every sensor also has *transmission rates  and a field that informs when the report starts*, but most important for each sensor is that has different **parameters** that defines `min, max, standard and average` of: `temperature, cold pressure and hot pressure` and **for each** `3 levels of high, 3 levels of low and 1 level for optimal state`.

Imortant evaluate about time.

##### Observations:
 - Some vehicles has more than a device with diferent id (max 2) and some vehicles doesn't have a valid device_id with id `None`
 - Each registry consider 1 day because the transmititing_dur can be max 86400 considering the 24 hours on seconds

KPI:
__________________

- Number of sensors at extreme levels (High 3 or Low 3)
= alerts requiring immediate intervention

- Sensor distribution by level:
    - % at OPTIMAL level (1 level)
    - % at LOW level (for each 3 levels)
    - % at HIGH level (for each 3 levels)
    - % with no data / failure

- Percentage of time the vehicle has active communication
= (time with valid transmission / total expected time) × 100
 by **day** and by **month**

- Percentage of active sensors per vehicle
= (sensors with recent transmission / total sensors of vehicle) × 100
 by **day** and by **month**
______________
 Questions to solve:
1. What's the main goal of a device?
2. The sensors transmit information, as the device, but is in real time or has a middleware?.
3. I can assume that the dur postfix means duration and is for the time on that state, but What's cnt postfix (count?)?

