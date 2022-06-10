-- Creation of user_logins table

CREATE TABLE IF NOT EXISTS user_logins(
    user_id             varchar(128),
    device_type         varchar(32),
    masked_ip           varchar(256),
    masked_device_id    varchar(256),
    locale              varchar(32),
    app_version         varchar(32),
    create_date         date
);

-- ******NOTE********
-- The data type for app version was changed to a varchar(32) from integer.
-- There are records formatted as 0.2.4 which could be converted to integers directly
-- during the transformation process. Alternatively the periods could have been stripped
-- if it needed to truly be stored as an integer.