CREATE TABLE time_in_level_device (
    id integer NOT NULL identity(1,1) ENCODE az64,
    vehicle_id integer ENCODE raw,
    device_id integer ENCODE raw,
    vehicle_out_of_service smallint ENCODE az64,
    vehicle_maintenance smallint ENCODE az64,
    transmitting_dur integer ENCODE az64,
    not_transmitting_dur integer ENCODE az64,
    transmission_level smallint ENCODE az64,
    report_start_at timestamp without time zone ENCODE raw,
    updated_at timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone ENCODE az64,
    PRIMARY KEY (id)
)
DISTSTYLE AUTO
SORTKEY ( report_start_at, vehicle_id, device_id );