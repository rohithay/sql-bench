# bench

## 🌱 Features

### Run a query and see
```
bench query <sql>
```
* Execution time
* Processed bytes

### Perform a dry run
```
bench dryrun <sql>
```
* Check syntax
* Show bytes scanned
* Exit 1 if error

### Get schema for a table
```
bench schema <table>
```
* As JSON or table
* Optionally type check a result JSON against schema

### Show schema diff
```
bench diff <table1> <table2>
```
* Additions, deletions, type mismatches

### Lint `.sql` files
```
bench lint <file.sql>
```
