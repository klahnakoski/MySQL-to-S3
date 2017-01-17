# MySQL-to-S3
Connects to a database, explores the relations, and extracts the fact table 
as JSON documents, and dumps to S3 (or a file).

## Objective
A database can be a complex graph of relations, but there is usually only a 
few tables that represent the database's facts. This software de-normalizes 
the database, with respect to these *fact tables*, which results in the 
hierarchical set of relations that we will call a "snowflake" schema.

The snowflake schema is used to generate a JSON document for each record in the 
fact table. Many-to-one relations (lookup tables) are represented as inner 
objects, One-to-many relations (children) are represented as nested objects.


## Denormalization

The denormalization process involves walking all foreign key paths, breadth first and without cycles, from the fact table outward.  This creates a snowflake subset-schema from the original database. This walk requires some guidance to make the JSON beautiful.

Each fact table is uses a configuration file to control the denormalization process. There are three major properties.  I will use the Treeherder job extract configuration as an example:  
 
###Extract

Controls the what records get pulled, the size of the batch, and how to name those batches

	"extract": {
		"last": "output/treeherder_last_run.json",
		"field": ["last_modified", "id"],
		"type": ["time", "number"],
		"start": ["1jan2015", 0],
		"batch": ["day", 10000],
		"ids": "select last_modified, id from job where last_modified>'{{last.last_modified}}' or (last_modified>'{{last.last_modified}}' and id>={{last.id}}) order by last_modified, id limit 10001"
	}

* **`last`** - *string* - the name of the file to store the first record of the next batch
* **`field`** - `strings` - Field to track between extracts; it should be a timestamp, or constantly increasing value, that can help find all changes since the last run. This extract program will record the maximum value seen to the file system so subsequent runs can continue where it left off.
* **`type`** - `strings` - The type of field (either `time` or `number`)
* **`start`** - `strings` - The minimum value for the field expected. Used to start a new extract, and used to know what value to assign to zero
* **`batch`** - `strings` - size of the batch. For `time` this can be a duration.
* **`ids`** - `strings` - An important piece of SQL that will produce the set of keys to extract from the fact table.  This allows you to specify any query that can leverage indexes to increase performance. You can use the properties of the `last` document.

###Destination

Where the batches of documents are placed. 


`destination` can be a file name instead of a S3 configuration object (see `tests/resources/config` for examples).

	"destination": {
		"bucket": "active-data-treeherder-jobs",
		"public": true,
		"key_format": "a.b",
		"$ref": "file://~/private.json#aws_credentials"
	}

* **`bucket`** - *string* - name of the bucket 
* **`public`** - *boolean* - if the files in bucket will be made public (default `false`)
* **`key_format`** - *string* - a dot-delimited example key. The length of the path must equal the number of field names used in the `extract.field` 
* **`aws_access_key_id`** - *string* - AWS connection info
* **`aws_secret_access_key`** - *string* - AWS connection info 
* **`region`** - *string* - AWS region 

###Snowflake

The `snowflake` object limits the relational walk used to determine the JSON document shape. Without adding limits, all unique relation paths will be traversed, resulting in large, and possibly redundant, documents. You can `exclude` tables entirely, or declare some tables are good for `reference_only`.  

	"snowflake": {
		"fact_table": "job",
		"show_foreign_keys": false,
		"null_values": [
			"-",
			"unknown",
			""
		],
		"add_relations": [
			"treeherder.job.option_collection_hash -> treeherder.option_collection.option_collection_hash"
		],
		"include": [
		],
		"exclude": [
			"runnable_job",
			"auth_user",
			"job_log",
			"text_log_step",
			"performance_datum",
			"performance_alert_summary",
			"performance_signature"
		],
		"reference_only": [
			"user.email",
			"repository.name",
			"machine_platform.platform",
			"failure_classification.name",
			"option.name"
		],
		"database": {
			"schema": "treeherder",
			"username": "activedata",
			"$ref": "~/private.json#treeherder"
		}

* **`fact_table`** - *required* - name of the table that represents the facts being pulled. **MySQL-to-S3** can generate nested documents, so you need not choose the finest grain if you are fine with large documents.  For Treeherder, we are interested in the `job` facts.
* **`show_foreign_keys`** - *default true* - Include the foreign key ids. This is useful if you require those ids for later synchronization. If you are only interested in the relationships, then they can be left out, and the JSON will be simpler.
* **`null_values`** - Some databases use a variety of values that indicate *no value*. The database `NULL` is always considered missing, and these values are mapped to `NULL` too.
* **`add_relations`** -  Relations are important for the denormalization. If your database is missing relations, you can add them here. They must be in `<schema>.<table>.<column> form; most missing relations cross schema boundaries.  **MySQL-to-S3** can reach across those boundaries for complete denormalization.
* **`exclude`** - Some tables are not needed: They may be irrelevant for the extraction process, or they may contain sensitive information, or you may not have permissions to access the contents. In all these cases, the tables can be added to this list. For the Treeherder example, there are many `exclude` entries; this is to avoid pulling the Perfherder facts, which we pull using separate configuration.
* **`reference_only`** - Tables can be used to lookup primitive values, or you are not interested in the full expressions of row in a table. In these cases you probably want the uid replaced with the canonical value that uid represents.  For example `user_id` refers to the `users` table, which has a `email` column. Every `user_id` column, can be represented with a `users` property having the `email` value. This effectively removes the relational ids and simplifies the JSON.  If just the table is named, then it is included, but no nested documents will be attached.   
* **`database`** - required properties to connect to the database. Must include `schema` so that the `fact_table` name has context.
	
	
### Using Trace 

During a run, the "trace" will show all paths being traversed.  In this run I did not include all the `exclude`, and you can see the resulting pathology: The `push` table is used by both the `job` fact table, and the `performance_alert_summary` table; the foreign keys provide a path from `job` to `push.performance_alert_summary`.  If you want every push to include all the alert summaries that are related, you can certainly keep them, but for this scenario, it is too deep; much better to extract the `performance_alert_summary` separately.  For this reason we exclude the `performance*` tables.

	Trace .
	Trace job
	Trace job.failure_classification
	Trace job.option_collection
	Trace job.reference_data_signatures
	Trace job.build_platform
	Trace job.job_type
	Trace job.machine
	Trace job.machine_platform
	Trace job.product
	Trace job.push
	Trace job.repository
	Trace job.performance_datum
	Trace job.performance_datum
	Trace job.job_detail
	Trace job.bug_job_map
	Trace job.job_note
	Trace job.option_collection.option
	Trace job.option_collection.performance_signature
	Trace job.job_type.job_group
	Trace job.push.repository
	Trace job.push.commit
	Trace job.push.performance_alert_summary
	Trace job.push.performance_alert_summary
	Trace job.push.performance_datum
	Trace job.performance_datum.performance_signature
	Trace job.performance_datum.repository
	Trace job.job_note.failure_classification
	Trace job.option_collection.performance_signature.performance_signature
	Trace job.option_collection.performance_signature.performance_framework
	Trace job.option_collection.performance_signature.machine_platform
	Trace job.option_collection.performance_signature.repository
	Trace job.option_collection.performance_signature.performance_alert
	Trace job.push.performance_alert_summary.performance_framework
	Trace job.push.performance_alert_summary.repository
	Trace job.push.performance_alert_summary.performance_alert
	Trace job.push.performance_alert_summary.performance_alert
	Trace job.option_collection.performance_signature.performance_framework
	
Here is what it looks like when the `commit` is not excluded.

    "push": {
        "author": "jjong@mozilla.com",
        "commit": [
            {
                "author": "Jessica Jong <jjong@mozilla.com>",
                "comments": "Bug 1321245 - request native key bindings for delayed keypress events. r=smaug",
                "revision": "a8aec29e89a344aa6e14b393aacbfaef2ec6f86e"
            },
            {
                "author": "Jessica Jong <jjong@mozilla.com>",
                "comments": "try: -b do -p all -u all -t none",
                "revision": "065343694038fb04c3fa51223589206a0dcdbd07"
            }
        ],
        "repository": "try",
        "revision": "065343694038fb04c3fa51223589206a0dcdbd07",
        "revision_hash": "1d86ae4d89624716f7e25776734fecd20743da56",
        "time": 1480663991
    }

The commit information is readily available from ActiveData, so we do not need to repeat ourselves here. But, this is a personal call; hg.mozilla.org will eventually erase its knowledge of try pushes, so if you need the try comments maybe you should keep them here.  Alternately, you could consider the `commit` table yet-another-fact-table for separate extraction.  
