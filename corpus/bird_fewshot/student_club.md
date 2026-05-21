### Q
What is the highest amount of budget spend for an event?

### SQL
SELECT MAX(spent) FROM budget

### Q
What is the total amount of money spent for food?

### SQL
SELECT SUM(spent) FROM budget WHERE category = 'Food'
