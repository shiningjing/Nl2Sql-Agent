### Q
How many connections does the atom 19 have?

### SQL
SELECT COUNT(T.bond_id) FROM connected AS T WHERE SUBSTR(T.atom_id, -2) = '19'

### Q
What is the most common bond type?

### SQL
SELECT T.bond_type FROM ( SELECT bond_type, COUNT(bond_id) FROM bond GROUP BY bond_type ORDER BY COUNT(bond_id) DESC LIMIT 1 ) AS T
