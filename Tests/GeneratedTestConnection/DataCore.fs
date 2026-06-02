module GeneratedTestConnection.DataCore

open Microsoft.Data.SqlClient
open ADONetCodeGen.Core

/// Design-time connection to the localdb instance that TestSql deploys to.
/// Used both to generate the ADO.NET code and to run the tests against the database.
let [<Literal>] designTimeConn =
    @"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog=ADONetCodeGenTest;Integrated Security=True;Connect Timeout=30;MultipleActiveResultSets=False"

let [<Literal>] designTimeServer = @"(localdb)\MSSQLLocalDB"

/// Opens a connection to the test database, wrapped in an ISqlConnection.
let GetDbConn() =
    task {
        let sqlConnection = new SqlConnection(designTimeConn)
        do! sqlConnection.OpenAsync()
        return new SqlConn(sqlConnection)
    }

/// Opens a connection with an open transaction. Tests that mutate the database use this and
/// roll back at the end, keeping the persistent localdb instance clean across runs.
let GetDbConnInTransaction() =
    task {
        let sqlConnection = new SqlConnection(designTimeConn)
        do! sqlConnection.OpenAsync()
        let tran = sqlConnection.BeginTransaction()
        return new SqlConnWithTransaction(sqlConnection, tran)
    }
