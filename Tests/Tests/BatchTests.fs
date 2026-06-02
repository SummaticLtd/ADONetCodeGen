module Tests.BatchTests

open System.Collections.Immutable
open SimpleTests
open ADONetCodeGen.Core
open GeneratedADONET.dbo
open GeneratedTestConnection.DataCore

/// Two different commands combined into one batch with Batch.pair; the result is a tuple of
/// each component's result.
let private pairOfCommands =
    Test.Async("Batch.pair of a scalar UDF and a table UDF", fun () ->
        task {
            use! conn = GetDbConn()
            let batch = Batch.pair(AddOne.Command.AsBatch(AddOne.Input 41), NumbersUpTo.Command.AsBatch(NumbersUpTo.Input 2))
            let! (added, rows) = Command.executeBatch(conn, batch)
            Assert.Equal(ValueSome 42, added)
            Assert.Equal(2, rows.Length)
            Assert.Equal(1, rows.[0].Value)
            Assert.Equal(2, rows.[1].Value)
        })

/// The same command applied to many inputs as a single batch.
let private homogeneousScalar =
    Test.Async("Homogeneous batch of a scalar UDF over many inputs", fun () ->
        task {
            use! conn = GetDbConn()
            let data = ImmutableArray.Create(EchoInt.Input 10, EchoInt.Input 20, EchoInt.Input 30)
            let! results = Command.executeBatch(conn, EchoInt.Command.AsBatchHomogeneous(data))
            Assert.Equal(3, results.Length)
            Assert.Equal(ValueSome 10, results.[0])
            Assert.Equal(ValueSome 20, results.[1])
            Assert.Equal(ValueSome 30, results.[2])
        })

/// An empty batch must not touch the database (SqlBatch throws on zero commands); the runtime
/// short-circuits and returns the empty result.
let private emptyBatch =
    Test.Async("Empty homogeneous batch returns no results", fun () ->
        task {
            use! conn = GetDbConn()
            let! results = Command.executeBatch(conn, EchoInt.Command.AsBatchHomogeneous(ImmutableArray<EchoInt.Input>.Empty))
            Assert.Equal(0, results.Length)
        })

/// A non-query batch (many inserts in one round trip), run inside a rolled-back transaction.
let private homogeneousNonQuery =
    Test.Async("Homogeneous batch of inserts in a rolled-back transaction", fun () ->
        task {
            use! cwt = GetDbConnInTransaction()
            let data =
                ImmutableArray.Create(
                    InsertWidget.Input("A", 1.00m),
                    InsertWidget.Input("B", 2.00m),
                    InsertWidget.Input("C", 3.00m))
            do! Command.executeBatch(cwt, InsertWidget.Command.AsBatchHomogeneous(data))
            let! all = TableGetters.Widget.Command.Execute(cwt)
            Assert.Equal(3, all.Length)
            do! cwt.RollbackAsync()
        })

let BatchTestList =
    TestList("BatchTests", [ pairOfCommands; homogeneousScalar; emptyBatch; homogeneousNonQuery ])
