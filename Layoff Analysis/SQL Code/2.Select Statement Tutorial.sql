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