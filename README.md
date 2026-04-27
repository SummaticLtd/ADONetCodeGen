# ADONetCodeGen

ADONetCodeGen generates ADO.NET code to interface with an MSSQL database (SQL Server or Azure SQL Database) in a fast, type-safe, and trim-safe way.

It is similar in purpose to [SqlPlus](https://www.sqlplus.net/) and [Facil](https://github.com/cmeeren/Facil).

## Features

ADONetCodeGen supports:

- User-defined table types
- Stored procedures
- User-defined functions
- Batching, introduced in `Microsoft.Data.SqlClient` in 5.2.0 (2024)

## Usage

### Generating code

Use `ADONetCodeGen.Writer` to generate code. You need to provide:
- A design time connection string: e.g. @"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog=MyDB;Integrated Security=True"
- A design time server: e.g. @"(localdb)\MSSQLLocalDB"
- A namespace: e.g. "GeneratedADONET"

Call `ADONetCodeGen.Writer.getGeneratedFileLines(designTimeConn, designTimeServer, namespace)` to get the generated code, and write it to a file inside an F# project.

### Using the generated code

The generated code contains:
- User-defined table types, which are self-explanatory.
- One module for each Stored Procedure and UDF. Each module contains:
    - An `Input` type, if the command has parameters.
    - An `Output` type, if the command has a result set.
    - A `Command` object, which is of type `GenADO.StoredProcNonQuery`, `GenADO.StoredProcQuery`, `GenADO.TableUDF`, `GenADO.ScalarUDF`.

The commands take an `ISqlConnection` as input, which can be created via `SqlConn(conn: SqlConnection)` or `SqlConnWithTransaction`. This type ensures that at most one transaction can be active at a time, and allows the handling of transactions to be automatic.

The easiest way to execute one of these commands is via the extension methods, which you can see by typing `.` after the `Command` object. For example, if you have a stored procedure `DeleteUser`:

```fsharp
DeleteUser.Command.executeStoredProcNonQuery(conn, DeleteUser.Input(userId))
```

There is also a `TableGetters` module allowing, for each table, extraction of all rows in the table.

### Batching

You can create batches from Commands using `AsBatchSingle` or `AsBatchHomogenous` (applying the command to many data items), and batches can be combined using the batch module, and executed above via the extension method `executeBatch`.

## Setting up a build process

A good setup has:
- SQL defined in source control, e.g. in an SSDT project, giving some type-safety in SQL queries.
- A build of this SQL deploying to a database, usually a localdb instance.
- Generation of the dotnet code based on this deployed database.

An example workflow:
- An SSDT project which deploys to localdb on build.
- A test case which:
  - Analyzes the sql files in the SQL project and the .sqlproj itself and gets a hash.
  - Creates a first line "Sql hash: {hash}".
  - If the hash matches the first line of the generated code file, the test passes since the code is up to date.
  - If not, the test attempts to generate this code:
    - Checks that the database is deployed and up to date (by looking at the dacpac modified time and comparing to the last modified sql file time). If not then the test fails.
    - If the database is up to date, the test generates the code (and fails, with an instruction to commit changes and re-run).

## FAQs

### When should I use this project or Facil?

- Facil is very well tested, while currently ADONetCodeGen is only tested via database tests inside private SummaticLtd repos.
- Facil has more complete support for `SqlDbType`s while ADONetCodeGen only supports a common subset at present.
- ADONetCodeGen supports user-defined functions.
- ADONetCodeGen supports batching.
  
If you are missing a feature in ADONetCodeGen, please request one and contribute. It is an easy library to contribute to, since analysis and codegen are separated.

### Can I use this project from any dotnet language?

The generated code is in F#, but it can be referenced from any dotnet language.

If you would like to use it from C#, please open an issue and it will be easy to create a switch to allow generated code to use Nullables/Nullable Reference Types in preference to ValueOption types, making this code completely C#-friendly.