# MySQL-to-S3
Connects to a database, explores the relations, and extracts the fact table as JSON documents

## Objective
A database can be a complex web of relations, but there is usually one table that's the reason for its whole existence, which is called the *fact table*.  This software follows paths of relations, walking from one table to the next, until it gets to the edge of the database.  Each path is treated seperate, even if they overlap, and should the same table be found on two different paths, they are seen as two different tables.  This interprestion of the database schema is called denormalization, and results in a tree graph, or hierarchy, which we will call a "snowflake".

The snowflake schema is used to generate a JSON document for each record in the fact table.  Many-to-one relations (lookup table) are represented as inner objects, One-to-many relations (children) are represneted as nested objects.


