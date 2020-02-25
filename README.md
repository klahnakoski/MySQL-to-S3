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
 
### Extract

Controls the what records get pulled, the size of the batch, and how to name those batches

	"extract": {
		"threads":2,
		"last":"output/treeherder_last_run.json",
		"field":["last_modified","id"],
		"type":["time","number"],
		"start":["1jan2015",0],
		"batch":["day",1000]
	}

* **`threads`** - `integer` - number of threads used to process documents. Use 1 if you are debugging.
* **`last`** - `string` - the name of the file to store the first record of the next batch
* **`field`** - `strings` - Field to track between extracts; it should be a timestamp, or constantly increasing value, that can help find all changes since the last run. This extract program will record the maximum value seen to the file system so subsequent runs can continue where it left off.
* **`type`** - `strings` - The type of field (either `time` or `number`)
* **`start`** - `strings` - The minimum value for the field expected. Used to start a new extract, and used to know what value to assign to zero
* **`batch`** - `strings` - size of the batch. For `time` this can be a duration.

### Destination

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

### Snowflake

You can read more on [the `snowflake` configuration](https://github.com/klahnakoski/jx-mysql/blob/dev/docs/snowflake_extractor.md)
