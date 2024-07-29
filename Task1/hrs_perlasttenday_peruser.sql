/*################################################################################
# Filename: hrs_perlasttenday_peruser.sql
# Author: Denys Murynka
# Date: 2024-07-24
# Requirements:
#    - DB (BigQuery)
#
# Task: Запит для підрахунку сумарного часу, який кожен користувач провів на платформі за останні 10 днів
#
################################################################################*/

-- Generate the last 10 days, including the current day
WITH date_range AS (
    SELECT 
        DATE_SUB(CURRENT_DATE(), INTERVAL day DAY) AS action_date
    FROM 
        UNNEST(GENERATE_ARRAY(0, 9)) AS day
),
-- We extract all sessions for the last 10 days, converting the effective date to the DATE format
session_times AS (
    SELECT 
        id_user, 
        action, 
        DATE(TIMESTAMP_TRUNC(date_action, DAY)) AS action_date, 
        date_action 
    FROM 'DB_NAME.DATA_SET.TABLE_NAME'
    WHERE date_action >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 9 DAY)
),

-- Extract all open sessions
open_sessions AS (
    SELECT 
        id_user, 
        action_date, 
        date_action AS open_time 
    FROM session_times
    WHERE action = 'open'
),

-- We pull out all closed sessions
close_sessions AS (
    SELECT 
        id_user, 
        action_date, 
        date_action AS close_time 
    FROM session_times
    WHERE action = 'close'
),

-- We combine open and closed sessions for each user, calculating the time of each session
sessions AS (
    SELECT 
        o.id_user, 
        o.action_date, 
        o.open_time, 
        MIN(c.close_time) AS close_time
    FROM open_sessions o
    LEFT JOIN close_sessions c 
        ON o.id_user = c.id_user 
        AND o.action_date = c.action_date 
        AND c.close_time > o.open_time
    GROUP BY o.id_user, o.action_date, o.open_time
),

-- We calculate the total time spent on the platform for each user for each day
daily_sessions AS (
    SELECT 
        id_user, 
        action_date, 
        SUM(TIMESTAMP_DIFF(COALESCE(close_time, CURRENT_TIMESTAMP()), open_time, SECOND) / 3600) AS hours_online
    FROM sessions
    GROUP BY id_user, action_date
),

-- We create all possible combinations of dates and users
full_dates AS (
    SELECT 
        d.action_date, 
        u.id_user
    FROM date_range d
    CROSS JOIN (SELECT DISTINCT id_user FROM `DB_NAME.DATA_SET.TABLE_NAME`) u
)

-- We combine all possible combinations of dates and users with data on the time spent on the platform, filling the missing data with zeros
SELECT 
    fd.action_date, 
    fd.id_user, 
    COALESCE(ds.hours_online, 0) AS hours_online
FROM full_dates fd
LEFT JOIN daily_sessions ds 
    ON fd.id_user = ds.id_user 
    AND fd.action_date = ds.action_date
ORDER BY fd.action_date, fd.id_user;