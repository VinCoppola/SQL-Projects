SELECT * 
FROM layoffs_staging2;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

SELECT industry, SUM(Total_laid_off) as total_letgo
FROM layoffs_staging2
GROUP BY industry
ORDER BY total_letgo DESC;

SELECT country, industry, SUM(Total_laid_off) as total_letgo
FROM layoffs_staging2
GROUP BY country, industry
ORDER BY total_letgo DESC;

SELECT stage, SUM(Total_laid_off) as total_letgo
FROM layoffs_staging2
GROUP BY stage
ORDER BY total_letgo DESC;

SELECT stage, ROUND(AVG(percentage_laid_off),2) as percent_letgo
FROM layoffs_staging2
GROUP BY stage
ORDER BY percent_letgo DESC;

SELECT SUBSTRING(`date`, 1, 7) AS `month`,
SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
WHERE `date` IS NOT NULL -- use date since WHERE processes before SELECT ALiasing
GROUP BY `month`
ORDER BY `month` ASC;

WITH rolling_total AS
(
SELECT SUBSTRING(`date`, 1, 7) AS `month`,
SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
WHERE `date` IS NOT NULL -- use date since WHERE processes before SELECT ALiasing
GROUP BY `month`
ORDER BY `month` ASC
)
SELECT `month`,SUM(total_layoffs) OVER(ORDER BY `month`) AS "total_MoM"
FROM rolling_total;

SELECT company, YEAR(`date`), SUM(Total_laid_off) as total_letgo
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY total_letgo DESC;


WITH company_year(Company,years,total_letgo) AS
(
SELECT company, YEAR(`date`), SUM(Total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), COMPANY_YEAR_RANK AS
(SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_letgo DESC) as ranking
from COMPANY_YEAR
WHERE years is not null
)

SELECT *
FROM company_year_rank
WHERE ranking <= 5;

## https://www.kaggle.com/datasets/swaptr/layoffs-2022?resource=download source for other dataset