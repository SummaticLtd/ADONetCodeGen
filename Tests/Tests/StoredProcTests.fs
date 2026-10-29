module Tests.StoredProcTests

open System
open Microsoft.Data.SqlClient
open SimpleTests
open ADONetCodeGen.Core
open GeneratedADONET.dbo
open GeneratedTestConnection.DataCore

/// Exercises a non-query proc (insert, returns rows affected), a query proc (returns a typed
/// result set), and a table getter — all on one connection inside a transaction that is rolled
/// back, so the persistent localdb instance is left unchanged.
let private widgetProcs =
    Test.Async("Insert + query widgets in a rolled-back transaction", fun () ->
        task {
            use! cwt = GetDbConnInTransaction()

            let! n1 = InsertWidget.Command.Execute(cwt, InsertWidget.Input("Cheap", 5.00m))
            let! n2 = InsertWidget.Command.Execute(cwt, InsertWidget.Input("Pricey", 50.00m))
            Assert.Equal(1, n1)
            Assert.Equal(1, n2)

            let! over10 = GetWidgetsOver.Command.Execute(cwt, GetWidgetsOver.Input 10.00m)
            Assert.Equal(1, over10.Length)
            Assert.Equal("Pricey", over10.[0].Name)
            Assert.Equal(50.00m, over10.[0].Price)

            let! all = TableGetters.Widget.Command.Execute(cwt)
            Assert.Equal(2, all.Length)

            do! cwt.RollbackAsync()
        })

let private timeoutIsApplied =
    Test.Async("A timeout cuts off a slower command and allows a faster one", fun () ->
        task {
            let! timedOut =
                task {
                    use! conn = GetDbConn()
                    try
                        let! _ = WaitTwoSeconds.Command.Execute(conn, (), TimeSpan.FromSeconds 1.)
                        return false
                    with :? SqlException as e when e.Number = -2 ->
                        return true
                }
            Assert.True(timedOut, "Expected a timeout")
            use! conn = GetDbConn()
            let! _ = WaitTwoSeconds.Command.Execute(conn, (), TimeSpan.FromSeconds 5.)
            ()
        })

let StoredProcTestList =
    TestList("StoredProcTests", [ widgetProcs; timeoutIsApplied ])
