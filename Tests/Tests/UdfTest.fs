module Tests.UdfTest

open SimpleTests
open ADONetCodeGen.Core
open GeneratedADONET.dbo
open GeneratedTestConnection.DataCore

/// Calls the dbo.AddOne scalar UDF against the deployed localdb database
/// using the generated ADO.NET code, and checks the result.
let private addOneWorks =
    Test.Async("addOneWorks", fun () ->
        task {
            use! conn = GetDbConn()
            let! result = AddOne.Command.Execute(conn, AddOne.Input(41))
            Assert.Equal(ValueSome 42, result)
        }
    )

let UdfTestList =
    TestList("UdfTest", [ addOneWorks ])
