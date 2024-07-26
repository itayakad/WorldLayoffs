-- SETTING UP STAGING TABLE --
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- CHECKING FOR DUPLICATES --
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions
)
FROM layoffs_staging;
-- All duplicates labeled with row number greater than 1 --
WITH duplicate_cte AS(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions
) AS row_num
FROM layoffs_staging
)
DELETE
FROM duplicate_cte
WHERE row_num > 1;
-- Making new table w/o duplicates --
CREATE TABLE layoffs_staging2
LIKE layoffs_staging;

ALTER TABLE layoffs_staging2
ADD row_num int;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions
) AS row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;

-- STANDARDIZING --
SELECT company, TRIM(company)
FROM layoffs_staging2;
-- Eliminating white space from company names --
UPDATE layoffs_staging2
SET company = TRIM(company);
-- Switching blank and 'NULL' strings to NULL values in date, industry, ect --
UPDATE layoffs_staging2 
SET 
    industry = CASE 
        WHEN industry IS NULL OR industry = '' OR industry = 'NULL' THEN NULL 
        ELSE industry 
    END,
    total_laid_off = CASE 
        WHEN total_laid_off IS NULL OR total_laid_off = '' OR total_laid_off = 'NULL' THEN NULL 
        ELSE total_laid_off 
    END,
    percentage_laid_off = CASE 
        WHEN percentage_laid_off IS NULL OR percentage_laid_off = '' OR percentage_laid_off = 'NULL' THEN NULL 
        ELSE percentage_laid_off 
    END,
    funds_raised_millions = CASE 
        WHEN funds_raised_millions IS NULL OR funds_raised_millions = '' OR funds_raised_millions = 'NULL' THEN NULL 
        ELSE funds_raised_millions 
    END,
    date = CASE 
        WHEN date IS NULL OR date = '' OR date = 'NULL' THEN NULL 
        ELSE date 
    END;
-- Renaming industries, countries, ect with like names --
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%';
-- Switching date from text format to YYYY-MM-DD format --
SELECT date, company
FROM layoffs_staging2;

SELECT date,
STR_TO_DATE(date, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET date = STR_TO_DATE(date, '%m/%d/%Y');

SELECT *
FROM layoffs_staging2

ALTER TABLE layoffs_staging2
MODIFY COLUMN date DATE;

-- NULL/BLANK VALUES --
SELECT industry
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL;

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- REMOVING COMPANIES W/ NO REAL DATA --
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL and percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL and percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

-- EDA --
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

SELECT * # All of these companies went under, sorted by size
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT industry, SUM(total_laid_off) # Greatest layoffs by industry. Consumer & Retail market got destroyed. Possible relation to COVID?
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT MIN(date), MAX(date) # Shows range of layoffs. Starts with covid. Backs theory that COVID wrecked the Consumer & Retail market.
FROM layoffs_staging2;

SELECT company, SUM(total_laid_off) # Greatest lay offs by company, some of the biggest companies in the world today.
FROM layoffs_staging2
GROUP BY company 
ORDER BY 2 DESC;

SELECT SUBSTRING(date,1,7) AS 'month', SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(date,1,7) IS NOT NULL
GROUP BY month
ORDER BY 1 ASC;

WITH Rolling_Total AS( # Rolling total of layoffs
SELECT SUBSTRING(date,1,7) AS 'month', SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(date,1,7) IS NOT NULL
GROUP BY month
ORDER BY 1 ASC
)
SELECT month, total_off, SUM(total_off) OVER(ORDER BY month) AS rolling_total
FROM Rolling_Total;

SELECT company, YEAR(date), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(date)
ORDER BY 3 DESC;

WITH Company_Year (company, years, total_laid_off) AS( # Layoff by year, by company, ranked to see who were the 5 greatest layoffs per year
SELECT company, YEAR(date), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(date)
), Company_Year_Rank AS
(SELECT *,
DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5;
