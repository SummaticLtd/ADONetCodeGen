module Tests.StoredProcTests

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

let StoredProcTestList =
    TestList("StoredProcTests", [ widgetProcs ])
