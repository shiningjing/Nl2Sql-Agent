### Q
What is the grade span offered in the school with the highest longitude?

### SQL
SELECT GSoffered FROM schools ORDER BY ABS(longitude) DESC LIMIT 1

### Q
What is the postal street address for the school with the 7th highest Math average? Indicate the school's name.

### SQL
SELECT T2.MailStreet, T2.School FROM satscores AS T1 INNER JOIN schools AS T2 ON T1.cds = T2.CDSCode ORDER BY T1.AvgScrMath DESC LIMIT 6, 1
