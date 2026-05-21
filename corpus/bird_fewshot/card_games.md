### Q
What are the cards belong to duel deck a? List the ID.

### SQL
SELECT id FROM cards WHERE duelDeck = 'a'

### Q
How many cards have infinite power?

### SQL
SELECT COUNT(*) FROM cards WHERE power = '*'
