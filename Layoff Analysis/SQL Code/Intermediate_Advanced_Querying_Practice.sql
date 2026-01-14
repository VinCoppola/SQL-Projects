-- Practicing WINDOW Functions

USE parks_and_recreation;

SELECT gender, AVG(Salary) AS avg_salary
FROM employee_demographics dem
JOIN employee_salary sal
	ON dem.employee_id = sal.employee_id
GROUP BY gender;

SELECT dem.first_name, dem.last_name, gender, sal.salary, AVG(Salary) OVER(PARTITION BY gender) AS avg_gender_salary
FROM employee_demographics dem
JOIN employee_salary sal
	ON dem.employee_id = sal.employee_id;
    
SELECT dem.first_name, dem.last_name, gender, sal.salary, SUM(Salary) OVER(PARTITION BY gender) AS avg_gender_salary
FROM employee_demographics dem
JOIN employee_salary sal
	ON dem.employee_id = sal.employee_id
ORDER BY gender, first_name;

-- ROLLING TOTAL

SELECT dem.first_name, dem.last_name, gender, sal.salary, SUM(Salary) OVER(PARTITION BY gender order by dem.employee_id) AS avg_gender_salary
FROM employee_demographics dem
JOIN employee_salary sal
	ON dem.employee_id = sal.employee_id;    
    
-- IF NOT PARTITIONED
SELECT dem.first_name, dem.last_name, gender, sal.salary, SUM(Salary) OVER(order by dem.employee_id) AS avg_gender_salary
FROM employee_demographics dem
JOIN employee_salary sal
	ON dem.employee_id = sal.employee_id;
    
-- ROW NUMBER, RANK and DENSE_Rank
SELECT dem.first_name, dem.last_name, gender, sal.salary, 
Row_number() OVER(partition by gender order by salary DESC) AS row_num,
RANK() OVER(partition by gender order by salary DESC) AS rank_num,
Dense_rank() OVER(partition by gender order by salary DESC) AS dense_rank_num
FROM employee_demographics dem
JOIN employee_salary sal
	ON dem.employee_id = sal.employee_id;
    
    
-- Using CTEs
WITH CTE_example AS
(
SELECT gender, AVG(Salary) AS avg_sal, 
MAX(Salary) as max_sal,
MIN(Salary) as min_sal,
COUNT(Salary) as count_sal
FROM employee_demographics dem
JOIN employee_salary sal
	ON dem.employee_id = sal.employee_id
GROUP BY gender
)

SELECT AVG(avg_sal)
FROM CTE_Example;

-- Exploring how to do it with SUBQUERY BUT its much harder to itnerpret and read 
SELECT  AVG(avg_sal)
FROM 
(
SELECT gender, AVG(Salary) AS avg_sal, 
MAX(Salary) as max_sal,
MIN(Salary) as min_sal,
COUNT(Salary) as count_sal
FROM employee_demographics dem
JOIN employee_salary sal
	ON dem.employee_id = sal.employee_id
GROUP BY gender
) AS example_subquery;

WITH CTE_example AS
(
SELECT employee_id,
gender,
birth_date
FROM employee_demographics dem
WHERE birth_date > '1985-01-01'
),
CTE_example2 AS
(SELECT employee_id, salary
FROM employee_salary
WHERE salary > 50000
)

SELECT *
FROM CTE_Example as ct1
JOIN CTE_example2 as ct2
	ON ct1.employee_id = ct2.employee_id;

-- Example of adding aliases as parameters of the cte to avoid needing to do so / overwriting ones in query

WITH CTE_example(gender, avg_sal, max_sal, min_sal, count_sal) AS
(
SELECT gender, AVG(Salary), 
MAX(Salary) as not_used_name,
MIN(Salary),
COUNT(Salary)
FROM employee_demographics dem
JOIN employee_salary sal
	ON dem.employee_id = sal.employee_id
GROUP BY gender
)

SELECT *
FROM CTE_Example;


-- Practing Temporary Tables
# These last for session duration but will not be stored

CREATE TEMPORARY TABLE temp_table
(first_name VARCHAR(50),
last_name VARCHAR(50),
favoirte_movie VARCHAR(100)
);

INSERT INTO temp_table
VALUES("Vincenzo", "Coppola","Dead Poet's Society");


SELECT *
FROM temp_table;

-- MORE COMMON USE CASE
CREATE TEMPORARY TABLE salary_over_50k
SELECT * 
FROM employee_salary
WHERE salary >= 50000;

SELECT * 
FROM salary_over_50k;

-- Stored Procedures

CREATE PROCEDURE large_salaries()
SELECT * 
FROM employee_salary
WHERE salary >= 50000;

CALL large_salaries();

-- Above is not best practice
DROP PROCEDURE IF EXISTS large_salaries2;

DELIMITER $$
USE parks_and_recreation $$

CREATE PROCEDURE large_salaries2()
BEGIN
	SELECT * 
	FROM employee_salary
	WHERE salary >= 50000;
    SELECT * 
	FROM employee_salary
	WHERE salary >= 10000;
END $$

DELIMITER ; -- SET DELIMITER BACK TO DEFAULT SEMI COLON

CALL large_salaries2();

-- Parameters are just like function arguments

DROP PROCEDURE IF EXISTS salary_fetcher;

DELIMITER $$
USE parks_and_recreation $$

CREATE PROCEDURE salary_fetcher(p_id_num int)
BEGIN
	SELECT first_name, 
    last_name, 
    Salary 
	FROM employee_salary
	WHERE employee_id = p_id_num;
END $$

DELIMITER ; -- SET DELIMITER BACK TO DEFAULT SEMI COLON

CALL salary_fetcher(2);

-- Practing TRIGGERS & EVENTS
DELIMITER $$ 
CREATE TRIGGER employee_insert
	AFTER INSERT ON employee_salary
    FOR EACH ROW
BEGIN
INSERT INTO employee_demographics(employee_id, first_name, last_name)
VALUES (NEW.employee_id, NEW.first_name, NEW.last_name);
END $$ 

DELIMITER ;

INSERT INTO employee_salary(employee_id, first_name, last_name, occupation, salary, dept_id)
VALUES(14,'Vincenzo','Coppola','Entertainment CEO',1000000,NULL);

SELECT * FROM EMPLOYEE_SALARY;
SELECT * FROM employee_demographics;

-- EVENTS
-- These are Great for Automation
DELIMITER $$

CREATE EVENT delete_retirees
ON SCHEDULE EVERY 30 SECOND
DO
BEGIN
	DELETE
	FROM employee_demographics
    WHERE age >= 60;
END $$
DELIMITER ;

UPDATE employee_demographics
SET birth_date = '1960-08-24', age = 64
WHERE first_name = "Vincenzo";

SELECT *
FROM employee_demographics;

-- Confirmation....IT WORKED!!
