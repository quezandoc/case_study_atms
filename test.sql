SELECT d.vehicle_id, d.device_id, d.transmission_level as d_tr, d.transmitting_dur as d_tdur, d.not_transmitting_dur as d_ntdur, d.report_start_at as drsat, d.updated_at as duat,
    s.vehicle_id, s.sensor_id, s.transmission_level as s_tr, s.transmitting_dur as s_tdur, s.not_transmitting_dur as s_ntdur, s.report_start_at as ssat, s.updated_at as suat
FROM time_in_level_device AS d
JOIN time_in_level_sensor AS s ON d.vehicle_id = s.vehicle_id
WHERE vehicle_id = '{vehicle_id}' AND report_start_at >= '{start_date}' AND report_start_at < '{end_date}'
ORDER BY d.report_start_at DESC, s.report_start_at DESC;