Participant 1 - Kartik Yadav (yadav11)
Participant 2 - Jaideep Juneja (juneja0)

Participant 1 contribution - Implemented INSERT, optimized SELECT, UPDATE commands, CREATE INDEX 
Participant 2 contribution - Implmented CREATE INDEX, DROP INDEX, DESCRIBE, DELETE and optimized SELECT


Optimized SELECT -
Pushing Selects under Join
To avoid the relatively higher cost of a non-selected relation into a joing operation, we have implemented pushing selects for predicates that only are concerned to their respective relations.
Join Order changed by sorting the tables according to record counts(like mentioned in the book). The relations are nested in ascending order of their record counts within a join operation.