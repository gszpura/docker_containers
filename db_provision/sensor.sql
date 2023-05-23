DROP TABLE Measurement;

DROP TABLE Sensor;

DROP TABLE Location;

CREATE TABLE IF NOT EXISTS Location (
    id serial PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    address VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS Sensor (
    id uuid DEFAULT gen_random_uuid (),
	location_id int,
	type VARCHAR NOT NULL,
	PRIMARY KEY (id),
	FOREIGN KEY (location_id) REFERENCES "location" (id)
);

CREATE TABLE IF NOT EXISTS Measurement (
    id uuid DEFAULT gen_random_uuid (),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
	sensor_id uuid NOT NULL,
	value NUMERIC(10,4) NOT NULL,
	PRIMARY KEY (id),
	FOREIGN KEY (sensor_id) REFERENCES "sensor" (id)
);
