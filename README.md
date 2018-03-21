## The Minibase Project

##### Introduction

Minibase is a barebones relational database management system.
Its design is inspired from Minibase (RDBMS developed at University of Wisconsin).

**More details:** [here](http://research.cs.wisc.edu/coral/minibase/minibase.html)

---

Each layer is standalone and uses libraries for relatively lower layers to facilitate testing, incorporation and experimentation.

##### Standalone layers
- Storage (Physical Memory Manager and Buffer)
- Query Processor (Relational operator identification and comprehension)
- Query Optimizer (Planning and evaluation)

---
##### Storage
Supports accessing stored data on disk. BufferPool is the main class which supports other components in tuples insertion, deletion, page eviction, locks acquisition and release, transaction commit or abort. All the records are stored in HeapFile (heap data structure). It uses **LIFO** policy for buffer management.

##### Query Processor
Parser converts the query into a logical plan representation and then calls query optimizer to generate an optimal plan. Currently supported operations are SELECT, Natural JOIN, INSERT and DELETE.

##### Query Optimizer
implements selectivity estimation framework and cost-based optimizer.

##### Coming up
- Recovery and transaction mangement.
- Aggregates (COUNT, SUM, AVG, MIN, MAX)
- Locking Shared or exclusive lock on transaction's read or write request.
- Deadlock detection using dependency graph


