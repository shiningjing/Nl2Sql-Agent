### Q
For the female client who was born in 1976/1/29, which district did she opened her account?

### SQL
SELECT T1.A2 FROM district AS T1 INNER JOIN client AS T2 ON T1.district_id = T2.district_id WHERE T2.birth_date = '1976-01-29' AND T2.gender = 'F'

### Q
List out the no. of districts that have female average salary is more than 6000 but less than 10000?

### SQL
SELECT COUNT(DISTINCT T2.district_id)  FROM client AS T1 INNER JOIN district AS T2 ON T1.district_id = T2.district_id WHERE T1.gender = 'F' AND T2.A11 BETWEEN 6000 AND 10000
