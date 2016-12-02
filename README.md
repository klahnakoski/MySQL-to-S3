# MySQL-to-S3
Connects to a database, explores the relations, and extracts the fact table 
as JSON documents

## Objective
A database can be a complex web of relations, but there is usually one table 
that's the reason for its whole existence, which is called the *fact table*.  
This software de-normalizes the database, with respect to the fact table, which results in the hierarchical set of relations which we will call a "snowflake" schema.

The snowflake schema is used to generate a JSON document for each record in the 
fact table. Many-to-one relations (lookup table) are represented as inner 
objects, One-to-many relations (children) are represented as nested objects.

### Nomenclature

"Relational id" refers to a column, or set of columns, that's used in the foreign-key relations

## Denormalization

The denormalization process involves walking all foreign key paths, breadth first and without cycles, from the fact table outward.  This walk requires some guidance to make the JSON beautiful.

Each fact table is uses a configuration file to control the denormalization process. Here are the properties  

* `show_foreign_keys` - *default true* - Include the foreign key ids. This is useful if you require those ids for later synchronization. If you are only interested in the relationships, then they can be left out, and the JSON will be simpler for not having to hold those ids.
* `ids` - An important piece of SQL that will produce the set of keys to extract from the fact table.  This allows you to specify any query that can leverage indexes to increase performance.  You can use the name of the `prime_field`, which is the largest value of the previous successful extract.
* `prime_feld` - Field to track between extracts; it should be a timestamp, or constantly increasing value, that can help find all changes since the last run.  This extract program will record the maximum value seen to the file system so subsequent runs can continue where it left off. 
* `null_value` - Some databases use a variety of values that indicate *no value*. The database `NULL` is always considered missing, and these values are mapped to `NULL` too.
* `add_relations` -  Relations are important for the denormalization.  If your database is missing relations, you can add them here. 
* `exclude` - Some tables are not needed: They may be irrelevant for the extraction process, or they may contain sensitive information, or you may not have permissions to access the contents. In all these cases, the tables can be added to this list
* `reference_only` - Tables can be used to lookup primitive values, or you are not interested in the full expressions of row in a table. In these cases you probably want the uid replaced with the canonical value that uid represents.  For example `user_id` refers to the `users` table, which has a `email` column. Every `user_id` column, can be represented with a `users` property having the `email` value. This effectively removes the relational ids and simplifies the JSON.  
* `database` - required properties to connect to the database. Must include `schema` so that the `fact_table` name has context.


### Treeherder Example 

The extract for the Treeherder database is interested in the `job` facts. For this example, I added some missing relations for `performance_datum`, despite the fact it's being excluded.  

	{
		"fact_table": "job",
		"prime_field": "last_modified",
		"show_foreign_keys": false,
		"null_values": ["-", "unknown", ""],
		"ids": "select id from (select last_modified, id from job order by id desc limit 1000000) a order by id desc limit 10",
		"add_relations":[
			"treeherder.performance_datum.ds_job_id -> treeherder.job.project_specific_id",
			"treeherder.performance_datum.repository_id -> treeherder.job.repository_id"
		],
		"include": [
		],
		"exclude": [
			"runnable_job",
			"auth_user",
			"job_log",
			"text_log_step",
			"performance_datum",
			"commit"
		],
		"reference_only": [
			"user.email",
			"repository.name",
			"machine_platform.platform",
			"failure_classification.name"
		],
		"database": {
			"schema": "treeherder",
			"username": "activedata",
			"$ref":"~/private.json#treeherder"
		},
		"debug":{
			"trace":true
		}
	}
	
	
	
