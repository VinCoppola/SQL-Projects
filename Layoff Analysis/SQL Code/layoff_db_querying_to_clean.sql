## This File contains code used to inspect imported layoff data and alter any issues found including duplicates, nulls and blank values, etc. 
## Additionally created staging tables to ensure that original data remained available before standardizing aspects of data table as a whole
## This data will be used in Exploratory Data Analysis to follow in a new script named layoff_db_EDA

USE world_layoffs;

-- Imported from Initia CSV file
SELECT * 
FROM layoffs;

-- I wanted more context so I added data from another source, this could certainly lead to duplicates and redundancies so I want to clean well
SELECT * 
FROM layoffs_through_2025;

-- Data Cleaning, First create staging table
-- First Clearing Stage Tables for Insertion
DROP TABLE IF EXISTS layoffs_staging;
DROP TABLE IF EXISTS layoffs_25_staging;

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
SELECT COUNT(*) 
FROM layoffs_25_for_union;

SELECT COUNT(*) 
FROM layoffs_staging;

CREATE TABLE final_layoff_staging AS
(SELECT *
FROM layoffs_25_for_union
UNION 
SELECT * 
FROM layoffs_staging);

SELECT COUNT(*)
FROM final_layoff_staging;

UPDATE final_layoff_staging 
SET location = REPLACE(location, ', Non-U.S.',''),
percentage_laid_off = ROUND(percentage_laid_off,2);

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

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- Creating Table to officially delete duplicates
INSERT INTO layoffs_staging2
(SELECT *,
ROW_NUMBER() OVER(PARTITION BY 
company,location,industry,total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging);

DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

SELECT * 
FROM layoffs_staging2;

-- Standardizing Data

SELECT COMPANY, TRIM(Company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT(industry) 
FROM layoffs_staging2
ORDER BY 1;

SELECT * 
FROM layoffs_staging2
WHERE industry LIKE "Crypto%";

UPDATE layoffs_staging2
SET industry = "Crypto"
WHERE industry LIKE "Crypto%";

SELECT * 
FROM layoffs_staging2
WHERE industry IS NULL;

SELECT * 
FROM layoffs_staging2
WHERE industry = "";

SELECT * 
FROM layoffs_staging2
WHERE company IN (SELECT company 
FROM layoffs_staging2
WHERE industry = "" or industry IS NULL);

-- Not the most efficent way but used where to atleast decrease complexity
UPDATE layoffs_staging2
SET industry = CASE
	WHEN Company LIKE "Bally%" THEN "Entertainment"
    WHEN company = "Airbnb" THEN "Travel"
    WHEN company = "Carvana" THEN "Transportation"
    WHEN company = "Juul" THEN "Consumer"
END
WHERE industry = "" or industry IS NULL;

-- Alternative Option if there was more data to affect this is more efficient / performed with staging1
## UPDATE layoffs_staging
## SET industry = NULL 
## WHERE industry = '';

## SELECT *
## from layoffs_staging AS l1
## JOIN layoffs_staging AS l2
## 	ON l1.company = l2.company
## WHERE (l1.industry IS NULL) AND (l2.industry IS NOT NULL);

## UPDATE layoffs_staging AS l1
## JOIN layoffs_staging AS l2
## 	ON l1.company = l2.company
## SET l1.industry = l2.industry
## WHERE (l1.industry IS NULL) AND (l2.industry IS NOT NULL);

SELECT DISTINCT(location) 
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE location LIKE "%seldorf" OR location LIKE "Malm%";

SELECT DISTINCT(country) 
FROM layoffs_staging2
WHERE country LIKE "United States%";

UPDATE layoffs_staging2
SET location = CASE
	WHEN location LIKE "%seldorf" THEN "Dusseldorf"
    WHEN location LIKE "Malm%" THEN "Malmo"
END
WHERE location LIKE "%seldorf" OR location LIKE "Malm%";

UPDATE layoffs_staging2
SET country = TRIM(Trailing '.' FROM country)
WHERE country LIKE "United States%";

-- Date Standardization, (FIRST INSPECTING NULLS BUT NO DATE TO FILL)
SELECT * FROM layoffs_staging2
WHERE company IN (
SELECT COMPANY 
FROM layoffs_staging2
WHERE `DATE` IS NULL);

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`,"%m/%d/%Y");

SELECT * 
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Lets circle back to Nulls
SELECT * 
FROM layoffs_staging2 
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL
ORDER BY 1;

-- FOUND some redundant cases but impossible to know if the dublin branch is the same lay offs number as Toronto and other row is truly a duplicate after all
SELECT * 
FROM layoffs_staging2 AS l1
JOIN layoffs_staging2 AS l2
	ON l1.Company = l2.company AND l1.date = l2.date
WHERE l1.total_laid_off IS NULL AND l2.total_laid_off IS NOT NULL;

-- Im comfortable getting rid of this data for purposes of EDA 
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

-- Lets look at final table
SELECT * 
FROM layoffs_staging2;