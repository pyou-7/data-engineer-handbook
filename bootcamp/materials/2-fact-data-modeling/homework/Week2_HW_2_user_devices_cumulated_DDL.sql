DROP TABLE user_devices_cumulated;

CREATE TABLE user_devices_cumulated (
    user_id TEXT,
    browser_type TEXT,
    device_activity_datelist DATE[], -- Array of active dates per browser type
    date DATE,
    PRIMARY KEY (user_id, browser_type, date)
);

