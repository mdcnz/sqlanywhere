# SQLAnywhere Go Driver (Linux, CGO)

This repository is an implementation of a Go sql/driver to access [SQLAnywhere](http://dcx.sap.com/index.html#sqla170/en/) databases. It uses SQLAnywhere's C api via cgo. The driver is intended for linux based client applications.

## Compiling

**Important: This is a CGO enabled package, and depends on shared libraries to compile and run.**

Compilation requirements:

- cgo enabled
- environment variable CGO_LDFLAGS="-L /path/to/libs"
- gcc compiler present in path

Runtime requirements:

- environment variable LD_LIBRARY_PATH="/path/to/libs"

/path/to/libs is the full path to the directory containing sqlanywhere linux shared object library files. The file **libdbcapi_r.so** is an example.

The libraries are typically installed as part of the installation of sqlanywhere server. If you don't have an existing sqlanywhere server installation, you can install the time limited free trial [sqlanywhere developer edition](https://www.sap.com/cmp/td/sap-sql-anywhere-developer-edition-free-trial.html). In case the link is unreachable, a [direct download](https://storage.googleapis.com/sqlanywhere-driver/sqla17developerlinux.tar.gz) is available (~320Mb).

This project uses docker and make for development. The Makefile has targets to download the sqlanywhere v17 developer edition locally once and install it in a container for compilation and testing.

## Usage

Connect to a database with a standard sqlanywhere [connection string](http://dcx.sap.com/index.html#sqla170/en/html/8142fdf46ce21014b956e05a37e48df4.html).

For example:

```go
package main

import (
    _ "github.com/mdcnz/sqlanywhere"
    "database/sql"
    "log"
)

func main() {
    db, err := sql.Open("sqlanywhere", "uid=DBA;pwd=xxx;Host=myhost;DBN=mydb;Server=myserver")
    if err != nil {
        log.Fatalf("did not open: %v", err)
    }
    defer db.Close()

    err = db.Ping()
    if err != nil {
        log.Fatalf("did not ping: %v", err)
    }
}
```

### Running sqlanywhere server

Examples of starting a server in the background, and testing a connection using dbping:

```shell
dbspawn dbsrv17 -n sqlanywhere-db-server -su dba,sqlsql
```

```shell
dbping -d -c "uid=dba;pwd=sqlsql;server=sqlanywhere-db-server;dbn=utility_db"
```

### Troubleshooting

```shell
/usr/bin/ld: cannot find -ldbcapi_r

/usr/bin/ld: cannot find -l:libdbcapi_r.so
```

This can occur at compile time if CGO_LDFLAGS is unset or incorrect.

```shell
error while loading shared libraries: libdbcapi_r.so: cannot open shared object file: No such file or directory
```

This can occur at runtime if the LD_LIBRARY_PATH is unset or incorrect.
