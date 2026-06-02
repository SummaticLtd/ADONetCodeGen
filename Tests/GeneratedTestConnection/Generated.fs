// Sql Hash: 5ea9724adc1466d9
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
[<RequireQualifiedAccess>]
module EchoBigInt =
    type Input(value: int64) =
        member _.CommandParams =
            ImmutableArray.Create<SqlParameter>(
                SqlParameter("@value", SqlDbType.BigInt, Value = value)
            )
    let Command =
        GenADO.ScalarUDF<Input, int64 voption>(
            "SELECT [dbo].[EchoBigInt](@value)",
            (fun i -> i.CommandParams),
            (fun reader -> if reader.IsDBNull(0) then ValueNone else ValueSome(reader.GetInt64(0))))
[<RequireQualifiedAccess>]
module EchoBit =
    type Input(value: bool) =
        member _.CommandParams =
            ImmutableArray.Create<SqlParameter>(
                SqlParameter("@value", SqlDbType.Bit, Value = value)
            )
    let Command =
        GenADO.ScalarUDF<Input, bool voption>(
            "SELECT [dbo].[EchoBit](@value)",
            (fun i -> i.CommandParams),
            (fun reader -> if reader.IsDBNull(0) then ValueNone else ValueSome(reader.GetBoolean(0))))
[<RequireQualifiedAccess>]
module EchoDateTime2 =
    type Input(value: DateTime) =
        member _.CommandParams =
            ImmutableArray.Create<SqlParameter>(
                SqlParameter("@value", SqlDbType.DateTime2, Value = value)
            )
    let Command =
        GenADO.ScalarUDF<Input, DateTime voption>(
            "SELECT [dbo].[EchoDateTime2](@value)",
            (fun i -> i.CommandParams),
            (fun reader -> if reader.IsDBNull(0) then ValueNone else ValueSome(reader.GetDateTime(0))))
[<RequireQualifiedAccess>]
module EchoDateTimeOffset =
    type Input(value: DateTimeOffset) =
        member _.CommandParams =
            ImmutableArray.Create<SqlParameter>(
                SqlParameter("@value", SqlDbType.DateTimeOffset, Value = value)
            )
    let Command =
        GenADO.ScalarUDF<Input, DateTimeOffset voption>(
            "SELECT [dbo].[EchoDateTimeOffset](@value)",
            (fun i -> i.CommandParams),
            (fun reader -> if reader.IsDBNull(0) then ValueNone else ValueSome(reader.GetDateTimeOffset(0))))
[<RequireQualifiedAccess>]
module EchoDecimal =
    type Input(value: decimal) =
        member _.CommandParams =
            ImmutableArray.Create<SqlParameter>(
                SqlParameter("@value", SqlDbType.Decimal, Value = value)
            )
    let Command =
        GenADO.ScalarUDF<Input, decimal voption>(
            "SELECT [dbo].[EchoDecimal](@value)",
            (fun i -> i.CommandParams),
            (fun reader -> if reader.IsDBNull(0) then ValueNone else ValueSome(reader.GetDecimal(0))))
[<RequireQualifiedAccess>]
module EchoFloat =
    type Input(value: double) =
        member _.CommandParams =
            ImmutableArray.Create<SqlParameter>(
                SqlParameter("@value", SqlDbType.Float, Value = value)
            )
    let Command =
        GenADO.ScalarUDF<Input, double voption>(
            "SELECT [dbo].[EchoFloat](@value)",
            (fun i -> i.CommandParams),
            (fun reader -> if reader.IsDBNull(0) then ValueNone else ValueSome(reader.GetDouble(0))))
[<RequireQualifiedAccess>]
module EchoInt =
    type Input(value: int) =
        member _.CommandParams =
            ImmutableArray.Create<SqlParameter>(
                SqlParameter("@value", SqlDbType.Int, Value = value)
            )
    let Command =
        GenADO.ScalarUDF<Input, int voption>(
            "SELECT [dbo].[EchoInt](@value)",
            (fun i -> i.CommandParams),
            (fun reader -> if reader.IsDBNull(0) then ValueNone else ValueSome(reader.GetInt32(0))))
[<RequireQualifiedAccess>]
module EchoNVarChar =
    type Input(value: string) =
        member _.CommandParams =
            ImmutableArray.Create<SqlParameter>(
                SqlParameter("@value", SqlDbType.NVarChar, Value = value)
            )
    let Command =
        GenADO.ScalarUDF<Input, string voption>(
            "SELECT [dbo].[EchoNVarChar](@value)",
            (fun i -> i.CommandParams),
            (fun reader -> if reader.IsDBNull(0) then ValueNone else ValueSome(reader.GetString(0))))
[<RequireQualifiedAccess>]
module EchoNullableInt =
    type Input(value: int voption) =
        member _.CommandParams =
            ImmutableArray.Create<SqlParameter>(
                SqlParameter("@value", SqlDbType.Int, Value = (match value with | ValueSome x -> box x | ValueNone -> box DBNull.Value))
            )
    let Command =
        GenADO.ScalarUDF<Input, int voption>(
            "SELECT [dbo].[EchoNullableInt](@value)",
            (fun i -> i.CommandParams),
            (fun reader -> if reader.IsDBNull(0) then ValueNone else ValueSome(reader.GetInt32(0))))
[<RequireQualifiedAccess>]
module EchoReal =
    type Input(value: single) =
        member _.CommandParams =
            ImmutableArray.Create<SqlParameter>(
                SqlParameter("@value", SqlDbType.Real, Value = value)
            )
    let Command =
        GenADO.ScalarUDF<Input, single voption>(
            "SELECT [dbo].[EchoReal](@value)",
            (fun i -> i.CommandParams),
            (fun reader -> if reader.IsDBNull(0) then ValueNone else ValueSome(reader.GetFloat(0))))
[<RequireQualifiedAccess>]
module EchoSmallInt =
    type Input(value: int16) =
        member _.CommandParams =
            ImmutableArray.Create<SqlParameter>(
                SqlParameter("@value", SqlDbType.SmallInt, Value = value)
            )
    let Command =
        GenADO.ScalarUDF<Input, int16 voption>(
            "SELECT [dbo].[EchoSmallInt](@value)",
            (fun i -> i.CommandParams),
            (fun reader -> if reader.IsDBNull(0) then ValueNone else ValueSome(reader.GetInt16(0))))
[<RequireQualifiedAccess>]
module EchoTime =
    type Input(value: TimeSpan) =
        member _.CommandParams =
            ImmutableArray.Create<SqlParameter>(
                SqlParameter("@value", SqlDbType.Time, Value = value)
            )
    let Command =
        GenADO.ScalarUDF<Input, TimeSpan voption>(
            "SELECT [dbo].[EchoTime](@value)",
            (fun i -> i.CommandParams),
            (fun reader -> if reader.IsDBNull(0) then ValueNone else ValueSome(reader.GetTimeSpan(0))))
[<RequireQualifiedAccess>]
module EchoTinyInt =
    type Input(value: byte) =
        member _.CommandParams =
            ImmutableArray.Create<SqlParameter>(
                SqlParameter("@value", SqlDbType.TinyInt, Value = value)
            )
    let Command =
        GenADO.ScalarUDF<Input, byte voption>(
            "SELECT [dbo].[EchoTinyInt](@value)",
            (fun i -> i.CommandParams),
            (fun reader -> if reader.IsDBNull(0) then ValueNone else ValueSome(reader.GetByte(0))))
[<RequireQualifiedAccess>]
module EchoUniqueIdentifier =
    type Input(value: Guid) =
        member _.CommandParams =
            ImmutableArray.Create<SqlParameter>(
                SqlParameter("@value", SqlDbType.UniqueIdentifier, Value = value)
            )
    let Command =
        GenADO.ScalarUDF<Input, Guid voption>(
            "SELECT [dbo].[EchoUniqueIdentifier](@value)",
            (fun i -> i.CommandParams),
            (fun reader -> if reader.IsDBNull(0) then ValueNone else ValueSome(reader.GetGuid(0))))
[<RequireQualifiedAccess>]
module EchoVarBinary =
    type Input(value: byte[]) =
        member _.CommandParams =
            ImmutableArray.Create<SqlParameter>(
                SqlParameter("@value", SqlDbType.VarBinary, Value = value)
            )
    let Command =
        GenADO.ScalarUDF<Input, byte[] voption>(
            "SELECT [dbo].[EchoVarBinary](@value)",
            (fun i -> i.CommandParams),
            (fun reader -> if reader.IsDBNull(0) then ValueNone else ValueSome(reader.GetSqlBinary(0).Value)))
[<RequireQualifiedAccess>]
module ManyTypedInputs =
    type Input(theByte: byte, theInt: int, theBigInt: int64, theBit: bool, theDouble: double, theDecimal: decimal, theGuid: Guid, theText: string, theTime: TimeSpan, theStamp: DateTime) =
        member _.CommandParams =
            ImmutableArray.Create<SqlParameter>(
                SqlParameter("@theByte", SqlDbType.TinyInt, Value = theByte),
                SqlParameter("@theInt", SqlDbType.Int, Value = theInt),
                SqlParameter("@theBigInt", SqlDbType.BigInt, Value = theBigInt),
                SqlParameter("@theBit", SqlDbType.Bit, Value = theBit),
                SqlParameter("@theDouble", SqlDbType.Float, Value = theDouble),
                SqlParameter("@theDecimal", SqlDbType.Decimal, Value = theDecimal),
                SqlParameter("@theGuid", SqlDbType.UniqueIdentifier, Value = theGuid),
                SqlParameter("@theText", SqlDbType.NVarChar, Value = theText),
                SqlParameter("@theTime", SqlDbType.Time, Value = theTime),
                SqlParameter("@theStamp", SqlDbType.DateTime2, Value = theStamp)
            )
    let Command =
        GenADO.ScalarUDF<Input, bool voption>(
            "SELECT [dbo].[ManyTypedInputs](@theByte, @theInt, @theBigInt, @theBit, @theDouble, @theDecimal, @theGuid, @theText, @theTime, @theStamp)",
            (fun i -> i.CommandParams),
            (fun reader -> if reader.IsDBNull(0) then ValueNone else ValueSome(reader.GetBoolean(0))))
[<RequireQualifiedAccess>]
module NumbersUpTo =
    type Input(count: int) =
        member _.CommandParams =
            ImmutableArray.Create<SqlParameter>(
                SqlParameter("@count", SqlDbType.Int, Value = count)
            )
    type Output(Value: int, Square: int64, Label: string, Maybe: int voption) =
        member _.Value = Value
        member _.Square = Square
        member _.Label = Label
        member _.Maybe = Maybe
    let Command =
        GenADO.TableUDF<Input, Output>(
            "SELECT * FROM [dbo].[NumbersUpTo](@count)",
            (fun i -> i.CommandParams),
            (fun reader -> Output(reader.GetInt32(0), reader.GetInt64(1), reader.GetString(2), (if reader.IsDBNull(3) then ValueNone else ValueSome(reader.GetInt32(3))))))
