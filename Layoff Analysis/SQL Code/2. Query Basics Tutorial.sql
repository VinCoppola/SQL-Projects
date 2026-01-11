SELECT * 
FROM employee_demographics;

SELECT first_name, 
last_name,
birth_date,
age,
age+10 as '10_years_older'
FROM parks_and_recreation.employee_demographics;

SELECT distinct(gender)
FROM employee_demographics;

# Where Clause
SELECT * 
FROM employee_salary
WHERE salary >= 50000;

SELECT * 
FROM employee_demographics
WHERE gender != 'Female' 
AND (birth_date > '1985-01-01' OR employee_id < 10);

-- special characters % (many characters) and _ (single wildcard)
SELECT * 
FROM employee_demographics
WHERE birth_date LIKE '1989%';

-- GROUP BY
SELECT gender, AVG(age), MAX(age), MIN(age), COUNT(age)
FROM employee_demographics
GROUP BY gender;

-- Order BY 
SELECT *
FROM employee_demographics
ORDER BY gender, age DESC;

-- Having vs Where

SELECT gender, AVG(age)
FROM employee_demographics
GROUP BY gender
HAVING avg(age) > 40;

SELECT occupation, Avg(Salary) as avg_salary
FROM employee_salary
WHERE occupation LIKE "%manager%"
GROUP BY occupation
HAVING avg_salary > 75000;

-- LIMIT & ALIASING
SELECT * 
FROM employee_demographics
ORDER BY age DESC
LIMIT 2, 1; #Offset 2 take top 1

-- ALIASING I DO ALL THIS ANYWAYS

SELECT gender, AVG(age) as avg_age 
FROM employee_demographics
GROUP BY gender
HAVING avg_age > 40;

-- JOINS
SELECT edem.employee_id, edem.age, esal.occupation
FROM employee_demographics edem
INNER JOIN employee_salary esal
	ON edem.employee_id = esal.employee_id;
    
SELECT *
FROM employee_demographics edem
RIGHT JOIN employee_salary esal
	ON edem.employee_id = esal.employee_id;
    
SELECT esal1.employee_id AS emp_santa,
esal1.first_name AS first_name_santa,
esal1.last_name AS last_name_santa,
esal2.employee_id AS emp_id,
esal2.first_name AS first_name,
esal2.last_name AS last_name
FROM employee_salary esal1
JOIN employee_salary esal2
	ON esal1.employee_id + 1 = esal2.employee_id;
    
SELECT *
FROM employee_demographics edem
INNER JOIN employee_salary esal
	ON edem.employee_id = esal.employee_id
INNER JOIN parks_departments pd
	ON esal.dept_id = pd.department_id;
    
-- UNIONS
SELECT first_name, last_name
FROM employee_demographics
UNION ALL -- default is unique values
SELECT first_name, last_name
FROM employee_salary;

SELECT first_name, last_name, 'Old Man' as Label
FROM employee_demographics 
WHERE age > 40 AND gender = "Male"
UNION 
SELECT first_name, last_name, 'Old Lady' as Label
FROM employee_demographics 
WHERE age > 40 AND gender = "Female"
UNION 
SELECT first_name, last_name, 'highly paid employee' as Label
FROM employee_salary 
WHERE salary > 70000
ORDER BY first_name, last_name;

-- String Functions
SELECT Length('Skyfall');

SELECT first_name, length(first_name) as name_length
FROM employee_demographics
ORDER BY name_length;

SELECT first_name, upper(first_name) as formatted_name
FROM employee_demographics;

SELECT TRIM('       SKY                ');

SELECT first_name, 
LEFT(first_name, 4),
RIGHT(first_name, 4),
substring(first_name,3,2),
birth_date,
substring(birth_date,6,2) AS 'Birth Month'
FROM employee_demographics;

SELECT first_name, REPLACE(first_name, 'a','z')
FROM employee_demographics;

SELECT first_name,LOCATE('An',first_name)
FROM employee_demographics;

SELECT first_name, last_name,
CONCAT(first_name, " " ,last_name) AS 'Full Name'
FROM employee_demographics;


-- CASE Statements
SELECT first_name,
last_name,
CASE
	WHEN age <= 30 THEN 'Young'
    WHEN age BETWEEN 31 and 50 THEN 'Old'
    WHEN age >= 50 THEN "On Death's Door"
END AS "age_range"
FROM employee_demographics;    

SELECT sal.first_name,
sal.last_name,
sal.salary,
pd.department_name,
CASE
	WHEN sal.salary < 50000 THEN sal.salary * 1.05
    WHEN sal.salary > 50000 THEN sal.salary * 1.07
END AS new_salary,
CASE 
	WHEN pd.department_name = "Finance" THEN sal.salary * .10
END AS "Bonus"
FROM employee_salary sal
LEFT JOIN parks_departments pd
ON sal.dept_id = pd.department_id;

-- SubQueries

SELECT  *
FROM employee_demographics
WHERE employee_id IN 
				(SELECT employee_id
					FROM employee_salary
                    WHERE dept_id = 1);

SELECT first_name,  salary, 
(SELECT AVG(salary)
FROM employee_salary) AS avg_salary
FROM employee_salary;


SELECT AVG(max_age)
FROM (SELECT gender, 
ROUND(AVG(age),2) avg_age, 
MAX(age) max_age, 
MIN(age) min_age, 
COUNT(age) freq_age
FROM employee_demographics
GROUP BY gender) AS agg_table;