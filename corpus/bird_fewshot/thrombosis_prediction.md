### Q
How many female patients were given an APS diagnosis?

### SQL
SELECT COUNT(ID) FROM Patient WHERE SEX = 'F' AND Diagnosis = 'APS'

### Q
What is the disease patient '30609' diagnosed with. List all the date of laboratory tests done for this patient.

### SQL
SELECT T1.Diagnosis, T2.Date FROM Patient AS T1 INNER JOIN Laboratory AS T2 ON T1.ID = T2.ID WHERE T1.ID = 30609
