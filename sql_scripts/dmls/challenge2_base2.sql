SELECT dh.department_id, dh.department, dh.hired, pe.prom_emp
                FROM (
                    SELECT 
                        d.id AS department_id, 
                        d.department, 
                        COUNT(*) AS hired
                    FROM hired_employees he
                    JOIN departments d ON he.department_id = d.id
                    WHERE YEAR(he.datetime) = 2021
                    GROUP BY d.id, d.department
                ) dh,
                (
                    SELECT AVG(p_count) AS prom_emp
                    FROM (
                        SELECT COUNT(1) AS p_count 
                        FROM hired_employees he
                        JOIN departments d ON he.department_id = d.id
                        WHERE YEAR(he.datetime) = 2021
                        GROUP BY d.id
                    ) hd
                ) pe
                WHERE dh.hired > pe.prom_emp;