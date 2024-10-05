CREATE DATABASE BANK;
USE BANK;


CREATE OR REPLACE TABLE DISTRICT(
District_Code INT PRIMARY KEY	,
District_Name VARCHAR(100)	,
Region VARCHAR(100)	,
No_of_inhabitants	INT,
No_of_municipalities_with_inhabitants_less_499 INT,
No_of_municipalities_with_inhabitants_500_btw_1999	INT,
No_of_municipalities_with_inhabitants_2000_btw_9999	INT,
No_of_municipalities_with_inhabitants_less_10000 INT,	
No_of_cities	INT,
Ratio_of_urban_inhabitants	FLOAT,
Average_salary	INT,
No_of_entrepreneurs_per_1000_inhabitants	INT,
No_committed_crime_2017	INT,
No_committed_crime_2018 INT
) ;

CREATE OR REPLACE TABLE ACCOUNT(
account_id INT PRIMARY KEY,
district_id	INT,
frequency	VARCHAR(40),
`Date` DATE ,
Account_type VARCHAR(40),
Card_Assigned VARCHAR(10),
FOREIGN KEY (district_id) references DISTRICT(District_Code) 
);

CREATE OR REPLACE TABLE `ORDER`(
order_id	INT PRIMARY KEY,
account_id	INT,
bank_to	VARCHAR(45),
account_to	INT,
amount FLOAT,
FOREIGN KEY (account_id) references ACCOUNT(account_id)
);



CREATE OR REPLACE TABLE LOAN(
loan_id	INT ,
account_id	INT,
--`Date`	DATE,
amount	INT,
duration	INT,
payments	INT,
`status` VARCHAR(35)
-- FOREIGN KEY (account_id) references ACCOUNT(account_id)
);

CREATE OR REPLACE TABLE TRANSACTIONS(
trans_id INT,	
account_id	INT,
`Date`	DATE,
`Type`	VARCHAR(30),
operation	VARCHAR(40),
amount	INT,
balance	FLOAT,
Purpose	VARCHAR(40),
bank	VARCHAR(45),
`account` INT,
FOREIGN KEY (account_id) references ACCOUNT(account_id));

CREATE OR REPLACE TABLE CLIENT(
client_id	INT PRIMARY KEY,
Sex	CHAR(10),
Birth_date	DATE,
district_id INT,
FOREIGN KEY (district_id) references DISTRICT(District_Code) 
);

CREATE OR REPLACE TABLE DISPOSITION(
disp_id	INT PRIMARY KEY,
client_id INT,
account_id	INT,
`type` CHAR(15),
FOREIGN KEY (account_id) references ACCOUNT(account_id),
FOREIGN KEY (client_id) references CLIENT(client_id)
);

CREATE OR REPLACE TABLE CARD(
card_id	INT PRIMARY KEY,
disp_id	INT,
`type` CHAR(10)	,
issued DATE,
FOREIGN KEY (disp_id) references DISPOSITION(disp_id)
);


CREATE OR REPLACE STORAGE integration s3_int
TYPE=EXTERNAL_STAGE
STORAGE_PROVIDER=s3
ENABLED=TRUE
STORAGE_AWS_ROLE_ARN='arn:aws:iam::798987017990:role/snowpipep_newuser_vp'
STORAGE_ALLOWED_LOCATIONS=('s3://snowpipep/')

Desc integration s3_int;

create or replace file format csv_format
TYPE='CSV'
FIELD_DELIMITER = ',' 
RECORD_DELIMITER = '\n' 
SKIP_HEADER=1;



CREATE OR REPLACE STAGE BANK
URL='s3://snowpipep'
file_format=csv_format
storage_integration=s3_int;

CREATE OR REPLACE PIPE BANK_SNOWPIPE_DISTRICT AUTO_INGEST=TRUE
AS COPY INTO "BANK"."PUBLIC"."DISTRICT" --  (table name that you created in snowflake)
FROM '@BANK/district/'   -- (name of the stage)
FILE_FORMAT = ( FORMAT_NAME = 'csv_format');

CREATE OR REPLACE PIPE BANK_SNOWPIPE_ACCOUNT AUTO_INGEST=TRUE
AS COPY INTO "BANK"."PUBLIC"."ACCOUNT" --  (table name that you created in snowflake)
FROM '@BANK/account/'   -- (name of the stage)
FILE_FORMAT = ( FORMAT_NAME = 'csv_format');

CREATE OR REPLACE PIPE BANK_SNOWPIPE_LOAN AUTO_INGEST=TRUE
AS COPY INTO "BANK"."PUBLIC"."LOAN" --  (table name that you created in snowflake)
FROM '@BANK/loan/'   -- (name of the stage)
FILE_FORMAT = ( FORMAT_NAME = 'csv_format');

CREATE OR REPLACE PIPE BANK_SNOWPIPE_TRANSACTIONS AUTO_INGEST=TRUE
AS COPY INTO "BANK"."PUBLIC"."TRANSACTIONS" --  (table name that you created in snowflake)
FROM '@BANK/Trnx/'   -- (name of the stage)
FILE_FORMAT = ( FORMAT_NAME = 'csv_format');

CREATE OR REPLACE PIPE BANK_SNOWPIPE_CARD
AUTO_INGEST=TRUE
AS COPY INTO "BANK"."PUBLIC"."CARD" --  (table name that you created in snowflake)
FROM '@BANK/card/'   -- (name of the stage)
FILE_FORMAT = ( FORMAT_NAME = 'csv_format');

CREATE OR REPLACE PIPE BANK_SNOWPIPE_CLIENT AUTO_INGEST=TRUE
AS COPY INTO "BANK"."PUBLIC"."CLIENT" --  (table name that you created in snowflake)
FROM '@BANK/client/'   -- (name of the stage)
FILE_FORMAT = ( FORMAT_NAME = 'csv_format');

CREATE OR REPLACE PIPE BANK_SNOWPIPE_DISPOSITION AUTO_INGEST=TRUE
AS COPY INTO "BANK"."PUBLIC"."DISPOSITION" --  (table name that you created in snowflake)
FROM '@BANK/disp/'   -- (name of the stage)
FILE_FORMAT = ( FORMAT_NAME = 'csv_format');


SHOW PIPES;

SELECT * FROM DISTRICT;
SELECT * FROM ACCOUNT;
SELECT * FROM LOAN;
SELECT COUNT(*) FROM CARD;
SELECT * FROM CLIENT;
SELECT COUNT(*) FROM DISPOSITION;
SELECT COUNT(*) FROM TRANSACTIONS;

--SELECT system$pipe_status('BANK_SNOWPIPE_ACCOUNT');
--SELECT system$pipe_status('BANK_SNOWPIPE_LOAN
-- Alter pipe BANK_SNOWPIPE_DISPOSITION refresh;

select * from `order`;

                                        --- Data Tranformation----
-- Adding new column Age to client

Alter table client Add Age int; 
select * from client;
UPDATE client 
set age=2022-year(birth_date);

-- Handling null values
Select * from transactions where bank is null and Year(`Date`)='2016';

Select Year(`date`) as TXN_YEAR, count(*) as TOT_TXN 
from transactions 
group by 1
ORDER BY 1;

/*
CONVERT 2021 TXN_YEAR TO 2022
CONVERT 2020 TXN_YEAR TO 2021
CONVERT 2018 TXN_YEAR TO 2020
CONVERT 2017 TXN_YEAR TO 2019
CONVERT 2016 TXN_YEAR TO 2018
*/

UPDATE TRANSACTIONS
SET `date`=DATEADD (YEAR,1,`date`)
WHERE YEAR(`date`)='2021' or YEAR(`date`)='2020';

UPDATE TRANSACTIONS
SET `date`=DATEADD (YEAR,2,`date`)
WHERE YEAR(`date`)='2018' or YEAR(`date`)='2017' or YEAR(`date`)='2016';

SELECT * FROM TRANSACTIONS WHERE BANK IS NULL AND YEAR(`DATE`)=2017;

UPDATE TRANSACTIONS
SET BANK='Sky Bank' WHERE BANK IS NULL AND YEAR(`DATE`)=2022;

UPDATE TRANSACTIONS
SET BANK='DBS Bank' WHERE BANK IS NULL AND YEAR(`DATE`)=2021;


UPDATE TRANSACTIONS
SET BANK='Northern Bank' WHERE BANK IS NULL AND YEAR(`DATE`)=2019;

UPDATE TRANSACTIONS
SET BANK='Southern Bank' WHERE BANK IS NULL AND YEAR(`DATE`)=2018;

UPDATE TRANSACTIONS
SET BANK='ADB Bank' WHERE BANK IS NULL AND YEAR(`DATE`)=2020;

SELECT * FROM TRANSACTIONS;
SELECT * FROM CARD;
SELECT DISTINCT YEAR(ISSUED) FROM CARD;

SELECT COUNT(*), YEAR(`DATE`) FROM ACCOUNT
GROUP BY 2
ORDER BY 2;
-------------  PRROBLEM STATEMENTS -------------

/*
1. What is the demographic profile of the bank's clients and how does it vary across
districts?
2. How the banks have performed over the years. Give their detailed analysis year &
month-wise.
3. What are the most common types of accounts and how do they differ in terms of usage
and profitability?
4. Which types of cards are most frequently used by the bank's clients and what is the
overall profitability of the credit card business?
5. What are the major expenses of the bank and how can they be reduced to improve
profitability?
6. What is the bank’s loan portfolio and how does it vary across different purposes and
client segments?
7. How can the bank improve its customer service and satisfaction levels?
8. Can the bank introduce new financial products or services to attract more customers and
increase profitability?

NOTE- 1 CZK=0.046735 USD
      1 CZK= 3.836706 INR

*/

---- Female vs male ratio -------
Select SEX, count(*)from client
group by 1;

-- 1. What is the demographic profile of the bank's clients and how does it vary across districts?--

CREATE OR REPLACE TABLE CZEC_Demographic AS
SELECT C.DISTRICT_ID,D.DISTRICT_NAME,D.AVERAGE_SALARY,
ROUND(AVG(C.AGE),0) AS AVG_AGE,
SUM(CASE WHEN SEX='Male' THEN 1 ELSE 0 END) AS MALE_CLIENT,
SUM(CASE WHEN SEX='Female' THEN 1 ELSE 0 END) AS FEMALE_CLIENT,
ROUND((FEMALE_CLIENT/MALE_CLIENT)*100,2) AS MALE_FEMALE_RATIO_PERC,
COUNT(*) AS TOTAL_CLIENT,
ROUND((FEMALE_CLIENT/TOTAL_CLIENT)*100,2) AS FEMALE_PERC,
ROUND((MALE_CLIENT/TOTAL_CLIENT)*100,2) AS MALE_PERC
FROM CLIENT C
INNER JOIN DISTRICT D ON C.DISTRICT_ID=D.DISTRICT_CODE
GROUP BY 1,2,3
ORDER BY 1;
SELECT * FROM CZEC_Demographic;
--2. How the banks have performed obver the years.Give their detailed analysis month wise?
--ASSUMING EVERY LAST MONTH CUSTOMER ACCOUNT IS GETTING TXNCTED

CREATE OR REPLACE TABLE ACC_LATEST_TXNS_WITH_BALANCE 
AS
SELECT LTD.*,TXN.BALANCE
FROM TRANSACTIONS AS TXN
INNER JOIN 
(
   SELECT ACCOUNT_ID,
   YEAR(`DATE`) AS TXN_YEAR,
   MONTH(`DATE`) AS TXN_MONTH,
   MAX(`DATE`) AS LATEST_TXN_DATE
   FROM TRANSACTIONS
   GROUP BY 1,2,3
   ORDER BY 1,2,3

) AS LTD ON TXN.ACCOUNT_ID = LTD.ACCOUNT_ID AND TXN.`DATE` = LTD.LATEST_TXN_DATE
WHERE TXN.`TYPE` = 'Credit' -- this is the assumptions am having : month end txn data is credit
ORDER BY TXN.ACCOUNT_ID,LTD.TXN_YEAR,LTD.TXN_MONTH;


select * from ACC_LATEST_TXNS_WITH_BALANCE;
-----------------------------------------------------------------------------------CREATING BANKING KPI-------------------------------------------------
CREATE OR REPLACE TABLE BANKING_KPI AS
SELECT 
ALWB.TXN_YEAR,
ALWB.TXN_MONTH,
T.BANK,
A.ACCOUNT_TYPE,
COUNT(DISTINCT ALWB.ACCOUNT_ID) AS TOT_ACCOUNT,
COUNT(DISTINCT T.TRANS_ID) AS TOT_TXNS,
COUNT(CASE WHEN T.`TYPE`='Credit' THEN 1 END ) AS DEPOSIT_COUNT,
COUNT(CASE WHEN T.`TYPE`='Withdrawal' THEN 1 END ) AS WITHDRAWAL_COUNT,
SUM(ALWB.BALANCE) AS TOT_BALANCE,
ROUND((DEPOSIT_COUNT / TOT_TXNS) * 100,2)  AS DEPOSIT_PERC ,
ROUND((WITHDRAWAL_COUNT / TOT_TXNS) * 100,2) AS WITHDRAWAL_PERC,
NVL(TOT_BALANCE / TOT_ACCOUNT,0) AS AVG_BALANCE,
ROUND(TOT_BALANCE/TOT_ACCOUNT,0) AS TPA
FROM TRANSACTIONS AS T
INNER JOIN ACC_LATEST_TXNS_WITH_BALANCE AS ALWB ON T.ACCOUNT_ID=ALWB.ACCOUNT_ID
LEFT OUTER JOIN  ACCOUNT AS A ON T.ACCOUNT_ID = A.ACCOUNT_ID
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4;



SELECT * FROM BANKING_KPI;
