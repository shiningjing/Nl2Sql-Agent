### Q
Is it true that more SMEs pay in Czech koruna than in euros? If so, how many more?

### SQL
SELECT SUM(Currency = 'CZK') - SUM(Currency = 'EUR') FROM customers WHERE Segment = 'SME'

### Q
How much did customer 6 consume in total between August and November 2013?

### SQL
SELECT SUM(Consumption) FROM yearmonth WHERE CustomerID = 6 AND Date BETWEEN '201308' AND '201311'
