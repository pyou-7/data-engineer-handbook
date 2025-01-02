CREATE TABLE hosts_cumulated (
    host_id TEXT,
    host_activity_datelist DATE[], -- Array of dates with activity
    date DATE,
    PRIMARY KEY (host_id, date)
);
