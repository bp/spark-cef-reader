# Spark CEF Data Source

A custom Spark data source supporting the [Common Event Format](https://support.citrix.com/article/CTX136146) standard
for logging events.

## Supported Features

* Schema inference. Uses data types for known extensions.
* Plain text, bzip2, and gzip files supported and tested
* Field pivoting, for turning `<key>Label` fields into the field names
* Scanning depth, allowing you to define how many records to scan to infer the schema
* Built using Spark DataSource v2 APIs
* Usage as a source in Spark SQL statements

## Usage

```scala
import com.bp.sds.cef._
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

// Read using provided data frame reader
val df = spark.read
  .option("maxRecords", 10000)  // Default 10,000
  .option("pivotFields", true)  // Default is false
  .cef("/path/to/file.log")

// Or using the format method (required for use in PySpark)

// Using the short format name
val dfShort = spark.read.format("cef").load("/path/to/file.log")

// Using the fully qualified name
val dfFull = spark.read.format("com.bp.sds.cef").load("/path/to/file.log")

// The path to the file may be an absolute path name, multiple path names, or a glob pattern.
val dfGlob = spark.read.cef("/landing/events/year=2020/month=*/day=*/*.log.gz")
```

Available for use in Spark SQL as well

```sql
SELECT
    *
FROM
    cef.`/path/to/file.log`
```

## Options

The following options are available to pass to the data source, where they are not defined then the default value
will be used.

Option | Type | Default | Purpose
------ | ---- | ------- | -------
maxRecords | Integer | 10,000 | The number of records to scan when inferring the schema. The data source will keep scanning until either the maximum number of records have been reached or there are no more files to scan.
pivotFields | Boolean | false | Scans for field pairs in the format of `key=value keyLabel=OtherKey` and pivots the data to `OtherKey=value`.
defensiveMode | Boolean | false | Used if a feed is known to violate the CEF spec. Adds overhead to the parsing so only use when there are known violations.
nullValue | String | `-` | A value used in the CEF records which should be parsed as a `null` value.
mode | ParseMode | Permissive | Permitted values are "permissive" and "failfast". When used in `FailFast` mode the parser will throw an error on the first record exception found. When used in `Permissive` mode it will attempt to parse as much of the record as possible, with `null` values used for all other values. `Permissive` mode may be used in combination with the `corruptRecordColumnName` option.
corruptRecordColumnName | String | `null` | When used with `Permissive` mode the full record is stored in a column with the name provided. If null is provided then the full record is discarded. By providing a name the data source will append a column to the infered schema.
