# bench
<p align="center">
  <a href="https://pypi.org/project/bench/"><img src="https://img.shields.io/pypi/v/bench.svg" alt="PyPI Version"></a>
  <a href="https://pypi.org/project/bench/"><img src="https://img.shields.io/pypi/pyversions/bench.svg" alt="Python Versions"></a>
  <a href="https://codecov.io/gh/rohithay/bench"><img src="https://codecov.io/gh/rohithay/bench/branch/main/graph/badge.svg" alt="Coverage Status"></a>
  <a href="https://github.com/rohithay/bench/blob/main/LICENSE"><img src="https://img.shields.io/github/license/rohithay/bench.svg" alt="License"></a>
</p>

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
