# Pivot_table

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.

---

This extension, pivot_table, allow you to pivot your data using a spreadsheet-like pivot API.
It is also similar to the Pandas `pivot_table` function. 
It does this solely through SQL macros - there are no C++ functions as a part of this extension.

Supporting this API means that depending on the parameters, sometimes the DuckDB `PIVOT` function is needed, and other times, a `GROUP BY` will suffice. 
This extension will dynamically generate the required SQL (in a manner that is safe from SQL injection) and then produce the desired output.

The main function is `pivot_table`, but it also relies on the creation of an enum `columns_parameter_enum` using the `build_my_enum` function.
For a full example, please see below.

The `pivot_table` function accepts the parameters:
* table_names
* values 
* rows
* columns
* filters
* subtotals
* grand_totals
* values_axis

This extension contains many tests, so it is designed to be useful as more than an example, but it is brand new as of 2024-09-16 so please be careful in production!


```sql
CREATE OR REPLACE TABLE business_metrics (
    product_line VARCHAR, product VARCHAR, year INTEGER, quarter VARCHAR, revenue integer, cost integer
);
INSERT INTO business_metrics VALUES
    ('Waterfowl watercraft', 'Duck boats', 2022, 'Q1', 100, 100),
    ('Waterfowl watercraft', 'Duck boats', 2022, 'Q2', 200, 100),
    ('Waterfowl watercraft', 'Duck boats', 2022, 'Q3', 300, 100),
    ('Waterfowl watercraft', 'Duck boats', 2022, 'Q4', 400, 100),
    ('Waterfowl watercraft', 'Duck boats', 2023, 'Q1', 500, 100),
    ('Waterfowl watercraft', 'Duck boats', 2023, 'Q2', 600, 100),
    ('Waterfowl watercraft', 'Duck boats', 2023, 'Q3', 700, 100),
    ('Waterfowl watercraft', 'Duck boats', 2023, 'Q4', 800, 100),

    ('Duck Duds', 'Duck suits', 2022, 'Q1', 10, 10),
    ('Duck Duds', 'Duck suits', 2022, 'Q2', 20, 10),
    ('Duck Duds', 'Duck suits', 2022, 'Q3', 30, 10),
    ('Duck Duds', 'Duck suits', 2022, 'Q4', 40, 10),
    ('Duck Duds', 'Duck suits', 2023, 'Q1', 50, 10),
    ('Duck Duds', 'Duck suits', 2023, 'Q2', 60, 10),
    ('Duck Duds', 'Duck suits', 2023, 'Q3', 70, 10),
    ('Duck Duds', 'Duck suits', 2023, 'Q4', 80, 10),

    ('Duck Duds', 'Duck neckties', 2022, 'Q1', 1, 1),
    ('Duck Duds', 'Duck neckties', 2022, 'Q2', 2, 1),
    ('Duck Duds', 'Duck neckties', 2022, 'Q3', 3, 1),
    ('Duck Duds', 'Duck neckties', 2022, 'Q4', 4, 1),
    ('Duck Duds', 'Duck neckties', 2023, 'Q1', 5, 1),
    ('Duck Duds', 'Duck neckties', 2023, 'Q2', 6, 1),
    ('Duck Duds', 'Duck neckties', 2023, 'Q3', 7, 1),
    ('Duck Duds', 'Duck neckties', 2023, 'Q4', 8, 1),
;

FROM business_metrics;
```

|     product_line     |    product    | year | quarter | revenue | cost |
|----------------------|---------------|-----:|---------|--------:|-----:|
| Waterfowl watercraft | Duck boats    | 2022 | Q1      | 100     | 100  |
| Waterfowl watercraft | Duck boats    | 2022 | Q2      | 200     | 100  |
| Waterfowl watercraft | Duck boats    | 2022 | Q3      | 300     | 100  |
| Waterfowl watercraft | Duck boats    | 2022 | Q4      | 400     | 100  |
| Waterfowl watercraft | Duck boats    | 2023 | Q1      | 500     | 100  |
| Waterfowl watercraft | Duck boats    | 2023 | Q2      | 600     | 100  |
| Waterfowl watercraft | Duck boats    | 2023 | Q3      | 700     | 100  |
| Waterfowl watercraft | Duck boats    | 2023 | Q4      | 800     | 100  |
| Duck Duds            | Duck suits    | 2022 | Q1      | 10      | 10   |
| Duck Duds            | Duck suits    | 2022 | Q2      | 20      | 10   |
| Duck Duds            | Duck suits    | 2022 | Q3      | 30      | 10   |
| Duck Duds            | Duck suits    | 2022 | Q4      | 40      | 10   |
| Duck Duds            | Duck suits    | 2023 | Q1      | 50      | 10   |
| Duck Duds            | Duck suits    | 2023 | Q2      | 60      | 10   |
| Duck Duds            | Duck suits    | 2023 | Q3      | 70      | 10   |
| Duck Duds            | Duck suits    | 2023 | Q4      | 80      | 10   |
| Duck Duds            | Duck neckties | 2022 | Q1      | 1       | 1    |
| Duck Duds            | Duck neckties | 2022 | Q2      | 2       | 1    |
| Duck Duds            | Duck neckties | 2022 | Q3      | 3       | 1    |
| Duck Duds            | Duck neckties | 2022 | Q4      | 4       | 1    |
| Duck Duds            | Duck neckties | 2023 | Q1      | 5       | 1    |
| Duck Duds            | Duck neckties | 2023 | Q2      | 6       | 1    |
| Duck Duds            | Duck neckties | 2023 | Q3      | 7       | 1    |
| Duck Duds            | Duck neckties | 2023 | Q4      | 8       | 1    |

```sql
DROP TYPE IF EXISTS columns_parameter_enum;

CREATE TYPE columns_parameter_enum AS ENUM (
    FROM build_my_enum(['business_metrics'], ['year', 'quarter'], [])
);

FROM pivot_table(['business_metrics'],
                 ['sum(revenue)', 'sum(cost)'],
                 ['product_line', 'product'],
                 ['year', 'quarter'],
                 [],
                 subtotals:=1,
                 grand_totals:=1,
                 values_axis:='rows'
                 );
```

|     product_line     |    product    | value_names  | 2022_Q1 | 2022_Q2 | 2022_Q3 | 2022_Q4 | 2023_Q1 | 2023_Q2 | 2023_Q3 | 2023_Q4 |
|----------------------|---------------|--------------|---------|---------|---------|---------|---------|---------|---------|---------|
| Duck Duds            | Duck neckties | sum(cost)    | 1       | 1       | 1       | 1       | 1       | 1       | 1       | 1       |
| Duck Duds            | Duck neckties | sum(revenue) | 1       | 2       | 3       | 4       | 5       | 6       | 7       | 8       |
| Duck Duds            | Duck suits    | sum(cost)    | 10      | 10      | 10      | 10      | 10      | 10      | 10      | 10      |
| Duck Duds            | Duck suits    | sum(revenue) | 10      | 20      | 30      | 40      | 50      | 60      | 70      | 80      |
| Duck Duds            | Subtotal      | sum(cost)    | 11      | 11      | 11      | 11      | 11      | 11      | 11      | 11      |
| Duck Duds            | Subtotal      | sum(revenue) | 11      | 22      | 33      | 44      | 55      | 66      | 77      | 88      |
| Waterfowl watercraft | Duck boats    | sum(cost)    | 100     | 100     | 100     | 100     | 100     | 100     | 100     | 100     |
| Waterfowl watercraft | Duck boats    | sum(revenue) | 100     | 200     | 300     | 400     | 500     | 600     | 700     | 800     |
| Waterfowl watercraft | Subtotal      | sum(cost)    | 100     | 100     | 100     | 100     | 100     | 100     | 100     | 100     |
| Waterfowl watercraft | Subtotal      | sum(revenue) | 100     | 200     | 300     | 400     | 500     | 600     | 700     | 800     |
| Grand Total          | Grand Total   | sum(cost)    | 111     | 111     | 111     | 111     | 111     | 111     | 111     | 111     |
| Grand Total          | Grand Total   | sum(revenue) | 111     | 222     | 333     | 444     | 555     | 666     | 777     | 888     |


**As a note, it may be best to wrap any use of these functions in a transaction using `BEGIN;` and `COMMIT;`, as the enum is a global object that can be edited concurrently.**

## Building
### Managing dependencies
DuckDB extensions uses VCPKG for dependency management. Enabling VCPKG is very simple: follow the [installation instructions](https://vcpkg.io/en/getting-started) or just run the following:
```shell
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```
Note: VCPKG is only required for extensions that want to rely on it for dependency management. If you want to develop an extension without dependencies, or want to do your own dependency management, just skip this step. Note that the example extension uses VCPKG to build with a dependency for instructive purposes, so when skipping this step the build may not work without removing the dependency.

### Build steps
Now to build the extension, run:
```sh
make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/pivot_table/pivot_table.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `pivot_table.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`.

Now we can use the features from the extension directly in DuckDB. 

Here is a basic example:
```
D create or replace temp table my_table as 
      SELECT x % 11 AS col1, x % 5 AS col2, x % 2 AS col3, 1 AS col4
      FROM range(1,101) t(x)
;
D select * FROM pivot_table(['my_table'],[], ['col3'], [], []);
┌───────┐
│ col3  │
│ int64 │
├───────┤
│     0 │
│     1 │
└───────┘
```

See above for a more complex example.

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

### Installing the deployed binaries
To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:
```shell
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension
you want to install. To do this run the following SQL query in DuckDB:
```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```
Note that the `/latest` path will allow you to install the latest extension version available for your current version of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:
```sql
INSTALL pivot_table
LOAD pivot_table
```
