module Tests.TableUdfTests

open SimpleTests
open ADONetCodeGen.Core
open GeneratedADONET.dbo
open GeneratedTestConnection.DataCore

/// Calls a table-valued UDF and checks the rows, the per-column types, and that a NULL
/// in the nullable column reads back as ValueNone.
let private numbersUpToReturnsRows =
    Test.Async("NumbersUpTo returns typed rows", fun () ->
        task {
            use! conn = GetDbConn()
            let! rows = NumbersUpTo.Command.Execute(conn, NumbersUpTo.Input 3)
            Assert.Equal(3, rows.Length)

            Assert.Equal(1, rows.[0].Value)
            Assert.Equal(1L, rows.[0].Square)
            Assert.Equal("n1", rows.[0].Label)
            Assert.Equal(ValueSome 10, rows.[0].Maybe)

            // Even values store NULL in Maybe -> ValueNone
            Assert.Equal(2, rows.[1].Value)
            Assert.Equal(4L, rows.[1].Square)
            Assert.Equal("n2", rows.[1].Label)
            Assert.Equal((ValueNone: int voption), rows.[1].Maybe)

            Assert.Equal(3, rows.[2].Value)
            Assert.Equal(9L, rows.[2].Square)
            Assert.Equal("n3", rows.[2].Label)
            Assert.Equal(ValueSome 30, rows.[2].Maybe)
        })

let private numbersUpToEmpty =
    Test.Async("NumbersUpTo with count 0 returns no rows", fun () ->
        task {
            use! conn = GetDbConn()
            let! rows = NumbersUpTo.Command.Execute(conn, NumbersUpTo.Input 0)
            Assert.Equal(0, rows.Length)
        })

let TableUdfTestList =
    TestList("TableUdfTests", [ numbersUpToReturnsRows; numbersUpToEmpty ])
