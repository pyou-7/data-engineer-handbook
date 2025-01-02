CREATE TABLE host_activity_reduced (
    host TEXT, -- The host identifier
    month DATE, -- The first day of the month
    hit_array INTEGER[], -- Array of daily hit counts
    unique_visitors INTEGER[], -- Array of daily unique visitors
    PRIMARY KEY (month, host) -- Ensures data is unique per host and month
);s