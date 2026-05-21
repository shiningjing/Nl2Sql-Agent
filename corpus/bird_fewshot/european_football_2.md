### Q
Which player is the tallest?

### SQL
SELECT player_name FROM Player ORDER BY height DESC LIMIT 1

### Q
List the football players with a birthyear of 1970 and a birthmonth of October.

### SQL
SELECT player_name FROM Player WHERE SUBSTR(birthday, 1, 7) = '1970-10'
