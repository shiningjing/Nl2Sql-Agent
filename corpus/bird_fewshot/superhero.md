### Q
What is the total number of superheroes without full name?

### SQL
SELECT COUNT(id) FROM superhero WHERE full_name IS NULL

### Q
Give the publisher ID of Star Trek.

### SQL
SELECT id FROM publisher WHERE publisher_name = 'Star Trek'
