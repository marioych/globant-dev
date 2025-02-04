SELECT department, job, q1, q2, q3, q4
                FROM
                (SELECT 
                    d.department,
                    j.job,
                    SUM(CASE WHEN MONTH(he.datetime) BETWEEN 1 AND 3 THEN 1 ELSE 0 END) AS Q1,
                    SUM(CASE WHEN MONTH(he.datetime) BETWEEN 4 AND 6 THEN 1 ELSE 0 END) AS Q2,
                    SUM(CASE WHEN MONTH(he.datetime) BETWEEN 7 AND 9 THEN 1 ELSE 0 END) AS Q3,
                    SUM(CASE WHEN MONTH(he.datetime) BETWEEN 10 AND 12 THEN 1 ELSE 0 END) AS Q4,
                    SUM(CASE WHEN MONTH(he.datetime) BETWEEN 1 AND 12 THEN 1 ELSE 0 END) AS TOTAL
                FROM hired_employees he
                JOIN departments d ON he.department_id = d.id
                JOIN jobs j ON he.job_id = j.id
                WHERE YEAR(he.datetime) = 2021
                GROUP BY d.department, j.job) a
                ORDER BY TOTAL DESC;