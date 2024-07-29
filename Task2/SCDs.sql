/*################################################################################
# Filename: SCDs.sql
# Author: Denys Murynka
# Date: 2024-07-25
# Requirements:
#    - DB (BigQuery)
#
# Task: Запит для підрахунку сумарного часу, який кожен користувач провів на платформі за останні 10 днів
# This task is designed to validate basic knowledge of Data Modelling Concepts.
# Notations:
# SCD1 - Slowly Changing Dimentions Type 1
# SCD2 - Slowly Chaning Dimentions Type 2
#
# See file Comments.pptx for comments on SCD changes!
#
################################################################################*/

-- CREATE REQUIRED TABLES
DROP TABLE IF EXISTS `DB_NAME.DATA_SET.dim_user`;
DROP TABLE IF EXISTS `DB_NAME.DATA_SET.stg_user`;

CREATE TABLE `DB_NAME.DATA_SET.dim_user`
(
    user_sk      STRING DEFAULT GENERATE_UUID() PRIMARY KEY NOT ENFORCED,
    user_bk      STRING,
    name         STRING, -- SCD1 attribute
    country      STRING, -- SCD2 attribute
    city         STRING, -- SCD2 attribute
    _valid_from  TIMESTAMP,
    _valid_to    TIMESTAMP
);

CREATE TABLE `DB_NAME.DATA_SET.stg_user`
(
    user_bk      STRING PRIMARY KEY NOT ENFORCED,
    name         STRING,
    country      STRING,
    city         STRING
);


-- INSERT INITIAL VALUES
INSERT INTO `DB_NAME.DATA_SET.dim_user`(user_bk, name, country, city, _valid_from, _valid_to)
VALUES 
    ('788d58fb', 'Myles', 'Canada', 'Torronto', '1000-01-01', '9999-12-31 23:59:59'),
    ('23bef18a', 'Neo', 'Ukraine', 'Ternopil', '1000-01-01', '9999-12-31 23:59:59'),
    ('6a94d22b', 'Kim', 'Poland', 'Warsaw', '1000-01-01', '9999-12-31 23:59:59'),
    ('8e7e4f9a', 'Ovan', 'France', 'Paris', '1000-01-01', '2023-05-05 23:59:59'),
    ('8e7e4f9a', 'Ovan', 'France', 'Leon', '2023-05-06', '9999-12-31 23:59:59');

-- INSERT NEW PORTION OF DATA
INSERT INTO `DB_NAME.DATA_SET.stg_user`(user_bk, name, country, city)
VALUES
('5da53bcd', 'Vasyl', 'USA', 'Los-Angeles'),
('8e7e4f9a', 'Ovaness', 'France', 'Nice'),
('23bef18a', 'Leopold', 'Ukraine', 'Ternopil'),
('6a94d22b', 'Kim', 'Poland', 'Warsaw'),
('788d58fb', 'Melisa', 'USA', 'New York');
       

-- TASK: Prepare SQL statements to correctly add new portion of data into the dim_user table
-- Explanations: We expect dim_user to have a valid historical records according to the specified in the table declaration attribute's SCD types


-- Update SCD1 attributes (name)
UPDATE `DB_NAME.DATA_SET.dim_user` du
SET 
    du.name = su.name
FROM `DB_NAME.DATA_SET.stg_user` su
WHERE du.user_bk = su.user_bk;

-- Close the current records by setting _valid_to to current timestamp
UPDATE `DB_NAME.DATA_SET.dim_user` du
SET 
    du._valid_to = CURRENT_TIMESTAMP()
FROM `DB_NAME.DATA_SET.stg_user` su
WHERE du.user_bk = su.user_bk
AND (du.country != su.country OR du.city != su.city)
AND du._valid_to = TIMESTAMP('9999-12-31 23:59:59');

-- Insert new versions for records that had their _valid_to updated
INSERT INTO `DB_NAME.DATA_SET.dim_user`(user_bk, name, country, city, _valid_from, _valid_to)
SELECT 
    su.user_bk, 
    su.name, 
    su.country, 
    su.city, 
    prev._valid_to, 
    TIMESTAMP('9999-12-31 23:59:59')
FROM `DB_NAME.DATA_SET.stg_user` su
JOIN (
    SELECT user_bk, MAX(_valid_to) AS _valid_to
    FROM `DB_NAME.DATA_SET.dim_user`
    WHERE CAST(_valid_to AS DATE) = CAST(CURRENT_TIMESTAMP() AS DATE)
    GROUP BY user_bk
) prev
ON su.user_bk = prev.user_bk;

-- Insert new records which do not exist in dim_user
INSERT INTO `DB_NAME.DATA_SET.dim_user`(user_bk, name, country, city, _valid_from, _valid_to)
SELECT 
    su.user_bk, 
    su.name, 
    su.country, 
    su.city, 
    CURRENT_TIMESTAMP(), 
    TIMESTAMP('9999-12-31 23:59:59')
FROM `DB_NAME.DATA_SET.stg_user` su
LEFT JOIN `DB_NAME.DATA_SET.dim_user` du
ON su.user_bk = du.user_bk
WHERE du.user_bk IS NULL;


