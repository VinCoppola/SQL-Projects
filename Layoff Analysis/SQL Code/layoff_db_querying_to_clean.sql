## This File contains code used to inspect imported layoff data and alter any issues found including duplicates, nulls and blank values, etc. 
## Additionally created staging tables to ensure that original data remained available before standardizing aspects of data table as a whole
## This data will be used in Exploratory Data Analysis to follow in a new script named layoff_db_EDA

DROP DATABASE IF EXISTS `world_layoffs`;
CREATE DATABASE `world_layoffs`;
USE world_layoffs;

-- Imported from Initial CSV file, check of data seems okay one date to look into - Avoiding SELECT(*) for efficiency 
DESCRIBE layoffs;

SELECT company, 
location,
country, 
total_laid_off, 
stage
FROM layoffs
LIMIT 15;

SELECT COUNT(*) as total_rows,
COUNT(company) as companies_entered,
COUNT(date) as dates_entered,
COUNT(location) as locations_entered
FROM layoffs;

-- This is ineffective as is so date column will need adjusting
## SELECT MIN(`date`) as first_day,
## MAX(`date`) as last_day
## FROM layoffs;

-- I wanted more context so I added data from another source, this data looks even better, this could certainly lead to duplicates and redundancies so I want to clean well
-- Additionally the lay off columns are listed as text so will need to adjust
DESCRIBE layoffs_through_2025;

SELECT company, 
location,
country, 
total_laid_off, 
stage
FROM layoffs_through_2025
LIMIT 15;

SELECT COUNT(*) as total_rows,
COUNT(company) as companies_entered,
COUNT(date) as dates_entered,
COUNT(location) as locations_entered
FROM layoffs_through_2025;

-- Data Cleaning, First create staging tables
-- First Clearing Stage Tables for Insertion
## DROP TABLE IF EXISTS layoffs_staging;
## DROP TABLE IF EXISTS layoffs_25_staging;

CREATE TABLE IF NOT EXISTS layoffs_staging
LIKE layoffs;

CREATE TABLE IF NOT EXISTS `layoffs_25_staging`
LIKE layoffs_through_2025;

INSERT INTO layoffs_staging
SELECT *
FROM layoffs;

INSERT INTO layoffs_25_staging
SELECT *
FROM layoffs_through_2025;

-- Here I'll inspect and remedy why the total_laid_off column wasn't imported as an INT so that it can match the original data before any merges
SELECT total_laid_off, COUNT(*) AS count_layoff
FROM layoffs_25_staging
WHERE total_laid_off IS NOT NULL AND total_laid_off NOT REGEXP '^[0-9]+$'
GROUP BY  total_laid_off;

-- Prep for and Perform Schema Change

UPDATE layoffs_25_staging
SET total_laid_off = NULLIF(TRIM(total_laid_off),'');

ALTER TABLE layoffs_25_staging
MODIFY COLUMN total_laid_off INT;

DESCRIBE layoffs_25_staging;

-- Now to combine the data, first creating a table for union which has only columns that exist in layoffs original
CREATE TABLE IF NOT EXISTS `layoffs_25_for_union`
LIKE layoffs_staging;

DROP PROCEDURE IF EXISTS select_shared_columns;

DELIMITER $$

CREATE PROCEDURE select_shared_columns()
BEGIN
    DECLARE column_list TEXT;
    DECLARE insert_list TEXT;
    DECLARE sql_stmt TEXT;

    -- Preventing GROUP_CONCAT truncation just in case (not as applicable for this dataset)
    SET SESSION group_concat_max_len = 100000;

    -- Build ordered list of shared columns
    SELECT
        GROUP_CONCAT(CONCAT('`', s.COLUMN_NAME, '`')
                     ORDER BY s.ORDINAL_POSITION)
    INTO column_list
    FROM INFORMATION_SCHEMA.COLUMNS s
    JOIN INFORMATION_SCHEMA.COLUMNS t
      ON s.COLUMN_NAME = t.COLUMN_NAME
     AND s.TABLE_SCHEMA = t.TABLE_SCHEMA
    WHERE s.TABLE_SCHEMA = 'world_layoffs'
      AND s.TABLE_NAME   = 'layoffs_staging'
      AND t.TABLE_NAME   = 'layoffs_25_staging';
      
      
      SELECT
        GROUP_CONCAT(
			CASE 
				WHEN s.DATA_TYPE IN ('int','bigint','smallint','mediumint','tinyint')
                THEN CONCAT('NULLIF(`', s.COLUMN_NAME, '`, '''') AS `',s.COLUMN_NAME,'`')
                ELSE CONCAT('`', s.COLUMN_NAME, '`')
                END
                     ORDER BY s.ORDINAL_POSITION)
    INTO insert_list
    FROM INFORMATION_SCHEMA.COLUMNS s
    JOIN INFORMATION_SCHEMA.COLUMNS t
      ON s.COLUMN_NAME = t.COLUMN_NAME
     AND s.TABLE_SCHEMA = t.TABLE_SCHEMA
    WHERE s.TABLE_SCHEMA = 'world_layoffs'
      AND s.TABLE_NAME   = 'layoffs_staging'
      AND t.TABLE_NAME   = 'layoffs_25_staging';

    -- Checking to see if there are no shared columns because that would be unideal in this scenario
    IF column_list IS NULL OR insert_list IS NULL THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'No shared columns found between tables';
    END IF;

    -- Build dynamic SQL
    SET sql_stmt = CONCAT(
        'Insert Into layoffs_25_for_union (',column_list,') ', 
        'SELECT ', insert_list, '
         FROM layoffs_25_staging'
    );

    -- Execute dynamic SQL
    SET @sql_to_execute = sql_stmt;

    PREPARE stmt FROM @sql_to_execute;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    SET @sql_to_execute = NULL;
    
END $$

DELIMITER ;

CALL select_shared_columns();

-- Confirming that row count hasnt changed and unioning data with original to
SELECT COUNT(company) 
FROM layoffs_25_for_union;

SELECT COUNT(company) 
FROM layoffs_staging;

-- DROP TABLE IF EXISTS final_layoff_staging;

CREATE TABLE final_layoff_staging AS
(SELECT *
FROM layoffs_25_for_union
UNION 
SELECT * 
FROM layoffs_staging);

SELECT COUNT(company)
FROM final_layoff_staging;

-- Now that its merged, I noticed some other issues with data types like date and percentages so here I will adjust
DESCRIBE final_layoff_staging;

SELECT DISTINCT percentage_laid_off
FROM final_layoff_staging
ORDER BY percentage_laid_off;

-- I want to make sure all my upates work properly or not at all so Ill do a transaction
START TRANSACTION;

UPDATE final_layoff_staging 
SET location = REPLACE(location, ', Non-U.S.','');

UPDATE final_layoff_staging 
SET percentage_laid_off = NULLIF(TRIM(percentage_laid_off),'');

UPDATE final_layoff_staging 
SET percentage_laid_off = ROUND(CAST(percentage_laid_off AS FLOAT),4);

UPDATE final_layoff_staging
SET `date` = str_to_date(`date`,"%m/%d/%Y");

COMMIT;

ALTER TABLE final_layoff_staging
MODIFY COLUMN percentage_laid_off DECIMAL(5,4);

ALTER TABLE final_layoff_staging
MODIFY COLUMN `date` DATE;

DESCRIBE final_layoff_staging;
-- Removing Duplicates First look into them
WITH duplicate_cte AS(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY 
company,location,industry,total_laid_off,percentage_laid_off,`date`, stage, country ORDER BY funds_raised_millions DESC) AS row_num
FROM final_layoff_staging)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;

-- Insepecting Some Weird Edge Cases
SELECT * 
FROM final_layoff_staging
WHERE company IN 
(
WITH duplicate_cte AS(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY 
company,location,industry,`date`, stage, country ORDER BY funds_raised_millions DESC) AS row_num
FROM final_layoff_staging)
SELECT DISTINCT(company) 
FROM duplicate_cte
WHERE row_num > 2
) 
ORDER BY Company,`date`,total_laid_off;

DROP TABLE IF EXISTS layoffs_staging2;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` DECIMAL(5,4),
  `date` date,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Creating Table to officially delete duplicates
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY 
company,location,industry,total_laid_off,percentage_laid_off,`date`, stage, country ORDER BY funds_raised_millions DESC) AS row_num
FROM final_layoff_staging;

DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

SELECT company, location, total_laid_off, funds_raised_millions 
FROM layoffs_staging2
LIMIT 15;

-- Standardizing Data

SELECT COMPANY, TRIM(Company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT(industry) 
FROM layoffs_staging2
ORDER BY 1;

SELECT company, industry, stage 
FROM layoffs_staging2
WHERE industry LIKE "Crypto%";

UPDATE layoffs_staging2
SET industry = "Crypto"
WHERE industry LIKE "Crypto%";

SELECT company 
FROM layoffs_staging2
WHERE industry IS NULL;

SELECT company,
industry,
location,
total_laid_off 
FROM layoffs_staging2
WHERE industry = "";

SELECT company,
industry,
location,
total_laid_off  
FROM layoffs_staging2
WHERE company IN (SELECT company 
FROM layoffs_staging2
WHERE industry = "" or industry IS NULL);

-- Not the most efficent way so decided to take alternate route below
## UPDATE layoffs_staging2
## SET industry = CASE
##	  WHEN Company LIKE "Bally%" THEN "Entertainment"
##    WHEN company = "Airbnb" THEN "Travel"
##    WHEN company = "Carvana" THEN "Transportation"
##    WHEN company = "Juul" THEN "Consumer"
## END
## WHERE industry = "" or industry IS NULL;

UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry = '';

UPDATE layoffs_staging2
SET stage = NULL 
WHERE stage = '';

SELECT l1.company
from layoffs_staging2 AS l1
JOIN layoffs_staging2 AS l2
ON l1.company = l2.company
WHERE (l1.industry IS NULL) AND (l2.industry IS NOT NULL);

UPDATE layoffs_staging2 AS l1
JOIN layoffs_staging2 AS l2
ON l1.company = l2.company
SET l1.industry = l2.industry
WHERE (l1.industry IS NULL) AND (l2.industry IS NOT NULL);

SELECT DISTINCT(location) 
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE location REGEXP "[^'A-Z a-z.-]";

## SELECT *
## FROM layoffs_staging2
## WHERE company LIKE "Deliveroo%";

UPDATE layoffs_staging2
SET location = CASE
	WHEN location LIKE "%seldorf" THEN "Dusseldorf"
    WHEN location LIKE "Malm%" THEN "Malmo"
    WHEN company = "Tibber" THEN "Forde"
    WHEN company = "The Org" THEN "New York"
    WHEN company = "Involves" THEN "Florianopolis"
    WHEN company = "Kleos Space" THEN "Kockelscheuer"
    WHEN company = "Deliveroo Australia" THEN "Melbourne"
    ELSE location
END;

SELECT DISTINCT(country) 
FROM layoffs_staging2
WHERE country LIKE "United States%";

UPDATE layoffs_staging2
SET country = TRIM(Trailing '.' FROM country)
WHERE country LIKE "United States%";

-- Date Standardization, (FIRST INSPECTING NULLS - had one company with null so did research to find time frame of when layoffs occured for Blackbaud to impute)
SELECT company,
date, 
total_laid_off
FROM layoffs_staging2
WHERE company IN (
SELECT COMPANY 
FROM layoffs_staging2
WHERE `DATE` IS NULL);

## UPDATE layoffs_staging2
## SET `date` = str_to_date(`date`,"%m/%d/%Y");
## Describe layoffs_staging2;

UPDATE layoffs_staging2
SET date = cast('2023-02-14' AS DATE)
WHERE company = "Blackbaud";

SELECT * 
FROM layoffs_staging2;

-- Lets circle back to Nulls
SELECT * 
FROM layoffs_staging2 
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL
ORDER BY 1;

-- THIS IS FOR ME TO PICK UP ON TOMORROW
-- FOUND some redundant cases but impossible to know if the dublin branch is the same lay offs number as Toronto and other row is truly a duplicate after all
SELECT * 
FROM layoffs_staging2 AS l1
JOIN layoffs_staging2 AS l2
	ON l1.Company = l2.company AND l1.date = l2.date
WHERE l1.total_laid_off IS NULL AND l2.total_laid_off IS NOT NULL;

-- Im comfortable getting rid of this data for purposes of EDA as we are focused on layoffs  
DELETE FROM layoffs_staging2 
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP Column row_num;

-- No staging companies to fill
SELECT DISTINCT(stage) 
from layoffs_staging2 
order by 1;

SELECT * 
FROM layoffs_staging2
WHERE company IN (
SELECT Company 
from layoffs_staging2 
WHERE stage IS NULL);

UPDATE layoffs_staging2 l1
JOIN layoffs_staging2 l2
ON l1.company = l2.company AND l1.`date` = l2.`date` 
SET l1.stage = l2.stage
WHERE l1.stage IS NULL and l2.stage IS NOT NULL;

SELECT * FROM 
layoffs_staging2 
WHERE company IN (SELECT DISTINCT(COMPANY)
FROM layoffs_staging2
WHERE stage = "unknown")
ORDER BY company,stage;

-- DELETING FINAL DUPLICATES TO ENSURE UNIQUENESS
SELECT *
FROM layoffs_staging2
WHERE COMPANY IN (WITH duplicate_cte AS (SELECT *,
ROW_NUMBER() OVER(Partition BY company, location, industry, `date`,country ORDER BY total_laid_off,percentage_laid_off,funds_raised_millions) AS row_num
FROM layoffs_staging2)
SELECT DISTINCT(company)  
FROM duplicate_cte
WHERE row_num > 2);

WITH duplicate_cte AS (SELECT *,
ROW_NUMBER() OVER(Partition BY company, location, industry, `date`,country ORDER BY total_laid_off,percentage_laid_off,funds_raised_millions) AS row_num
FROM layoffs_staging2)
SELECT *  
FROM duplicate_cte
WHERE row_num > 2;

UPDATE layoffs_staging2 l1
JOIN layoffs_staging2 l2
ON l1.company = l2.company AND l1.`date` = l2.`date` 
SET l1.percentage_laid_off = l2.percentage_laid_off
WHERE l1.percentage_laid_off IS NULL and l2.percentage_laid_off IS NOT NULL;

UPDATE layoffs_staging2 l1
JOIN layoffs_staging2 l2
ON l1.company = l2.company AND l1.`date` = l2.`date` 
SET l1.funds_raised_millions = l2.funds_raised_millions
WHERE l1.funds_raised_millions IS NULL and l2.funds_raised_millions IS NOT NULL;

-- Lets look at final table
SELECT * 
FROM layoffs_staging2;