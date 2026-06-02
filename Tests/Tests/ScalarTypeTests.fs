module Tests.ScalarTypeTests

open System
open System.Threading.Tasks
open SimpleTests
open ADONetCodeGen.Core
open GeneratedADONET.dbo
open GeneratedTestConnection.DataCore

// Shared representative values, chosen to round-trip exactly through SQL Server.
let private theGuid = Guid "12345678-1234-1234-1234-1234567890AB"
let private theStamp = DateTime(2024, 6, 1, 13, 45, 30, 123)
let private theOffset = DateTimeOffset(2024, 6, 1, 13, 45, 30, 123, TimeSpan.FromHours 2.0)
let private theTime = TimeSpan(0, 13, 45, 30, 123)

/// Sends `expected` of type 'a into an echo UDF and checks the same value comes back.
/// Scalar UDF returns are always nullable, so the result is an 'a voption.
let private roundTrip<'a when 'a: equality>
        (name: string, expected: 'a, run: ISqlConnection -> Task<'a voption>) =
    Test.Async(name, fun () ->
        task {
            use! conn = GetDbConn()
            let! actual = run conn
            Assert.Equal(ValueSome expected, actual)
        })

/// One echo round-trip per .NET-mapped scalar type the generator supports:
/// exercises both input-parameter mapping and scalar-output reading for each type.
let private echoTests = [
    roundTrip("tinyint",          200uy,                 fun c -> EchoTinyInt.Command.Execute(c, EchoTinyInt.Input 200uy))
    roundTrip("smallint",         -12345s,               fun c -> EchoSmallInt.Command.Execute(c, EchoSmallInt.Input -12345s))
    roundTrip("int",              1234567,               fun c -> EchoInt.Command.Execute(c, EchoInt.Input 1234567))
    roundTrip("bigint",           9876543210L,           fun c -> EchoBigInt.Command.Execute(c, EchoBigInt.Input 9876543210L))
    roundTrip("bit",              true,                  fun c -> EchoBit.Command.Execute(c, EchoBit.Input true))
    roundTrip("real",             1.5f,                  fun c -> EchoReal.Command.Execute(c, EchoReal.Input 1.5f))
    roundTrip("float",            3.141592653589793,     fun c -> EchoFloat.Command.Execute(c, EchoFloat.Input 3.141592653589793))
    roundTrip("decimal",          12345.6789m,           fun c -> EchoDecimal.Command.Execute(c, EchoDecimal.Input 12345.6789m))
    roundTrip("uniqueidentifier", theGuid,               fun c -> EchoUniqueIdentifier.Command.Execute(c, EchoUniqueIdentifier.Input theGuid))
    roundTrip("nvarchar",         "Hello, 世界",          fun c -> EchoNVarChar.Command.Execute(c, EchoNVarChar.Input "Hello, 世界"))
    roundTrip("varbinary",        [| 1uy; 2uy; 3uy; 255uy |], fun c -> EchoVarBinary.Command.Execute(c, EchoVarBinary.Input [| 1uy; 2uy; 3uy; 255uy |]))
    roundTrip("datetime2",        theStamp,              fun c -> EchoDateTime2.Command.Execute(c, EchoDateTime2.Input theStamp))
    roundTrip("datetimeoffset",   theOffset,             fun c -> EchoDateTimeOffset.Command.Execute(c, EchoDateTimeOffset.Input theOffset))
    roundTrip("time",             theTime,               fun c -> EchoTime.Command.Execute(c, EchoTime.Input theTime))
]

/// A single command taking many parameters of mixed types. The UDF returns true only
/// if every input arrived with its expected value, so this checks them all at once.
let private manyTypedInputs =
    Test.Async("manyTypedInputs", fun () ->
        task {
            use! conn = GetDbConn()
            let input =
                ManyTypedInputs.Input(
                    200uy, 1234567, 9876543210L, true, 3.141592653589793,
                    12345.6789m, theGuid, "Hello", theTime, theStamp)
            let! result = ManyTypedInputs.Command.Execute(conn, input)
            Assert.Equal(ValueSome true, result)
        })

/// A nullable input that is echoed back. The scalar return is always nullable, so this
/// covers both a nullable input (ValueNone -> NULL) and a nullable output (NULL -> ValueNone).
let private nullableRoundTrip =
    Test.Async("nullable int (Some and None)", fun () ->
        task {
            use! conn = GetDbConn()
            let! some = EchoNullableInt.Command.Execute(conn, EchoNullableInt.Input(ValueSome 42))
            Assert.Equal(ValueSome 42, some)
            let! none = EchoNullableInt.Command.Execute(conn, EchoNullableInt.Input ValueNone)
            Assert.Equal((ValueNone: int voption), none)
        })

let ScalarTypeTestList =
    TestList("ScalarTypeTests", echoTests @ [ manyTypedInputs; nullableRoundTrip ])
