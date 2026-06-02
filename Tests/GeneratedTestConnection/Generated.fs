// Sql Hash: ad4fd1a5901ce5a
// This code is auto-generated
namespace GeneratedADONET.dbo
open System
open System.Data
open Microsoft.Data.SqlClient
open System.Collections.Immutable
open ADONetCodeGen.Core
// ----------------------
// User Defined Functions
// ----------------------
[<RequireQualifiedAccess>]
module AddOne =
    type Input(n: int) =
        member _.CommandParams =
            ImmutableArray.Create<SqlParameter>(
                SqlParameter("@n", SqlDbType.Int, Value = n)
            )
    let Command =
        GenADO.ScalarUDF<Input, int voption>(
            "SELECT [dbo].[AddOne](@n)",
            (fun i -> i.CommandParams),
            (fun reader -> if reader.IsDBNull(0) then ValueNone else ValueSome(reader.GetInt32(0))))
