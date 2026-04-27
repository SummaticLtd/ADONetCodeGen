module ADONetCodeGen.Writer
open ADONetCodeGen.Analysis
open System.Collections.Immutable

[<RequireQualifiedAccess; Struct>]
type private FormatParameterOption =
    | Dotnet of includeType: bool * includePrecedingComma: bool
    | Sql

module private SqlType =
    let dotnetTypeStr(sqlType: SqlType) =
        match sqlType with
        | SqlType.Byte -> "byte"
        | SqlType.Int16 -> "int16"
        | SqlType.Int32 -> "int"
        | SqlType.Int64 -> "int64"
        | SqlType.DateTime -> "DateTime"
        | SqlType.Guid -> "Guid"
        | SqlType.String -> "string"
        | SqlType.Bool -> "bool"
        | SqlType.ByteArray -> "byte[]"
        | SqlType.Double -> "double"
        | SqlType.Decimal -> "decimal"
        | SqlType.UserDefinedTableType udtName -> $"ImmutableArray<{udtName}>"
    let getterStr(sqlType: SqlType, reader: string, index: int) =
        match sqlType with
        | SqlType.Byte -> $"{reader}.GetByte({index})"
        | SqlType.Int16 -> $"{reader}.GetInt16({index})"
        | SqlType.Int32 -> $"{reader}.GetInt32({index})"
        | SqlType.Int64 -> $"{reader}.GetInt64({index})"
        | SqlType.DateTime -> $"{reader}.GetDateTime({index})"
        | SqlType.Guid -> $"{reader}.GetGuid({index})"
        | SqlType.String -> $"{reader}.GetString({index})"
        | SqlType.Bool -> $"{reader}.GetBoolean({index})"
        | SqlType.ByteArray -> $"({reader}.GetValue({index}) :?> byte[])"
        | SqlType.Double -> $"{reader}.GetDouble({index})"
        | SqlType.Decimal -> $"{reader}.GetDecimal({index})"
        | SqlType.UserDefinedTableType udttName ->
            $"(let r = {reader}.GetData({index}) in {udttName}.FromReader(r))"

module private NamedType =
    let formatWithType(nt: NamedType) = nt.Name + ": " + SqlType.dotnetTypeStr(nt.SqlType) + (if nt.Nullable then " voption" else "")
    let formatParameters(parameters: NamedType seq, fpo: FormatParameterOption) =
        let strs =
            parameters |> Seq.map(fun p ->
                match fpo with
                | FormatParameterOption.Dotnet(includeTypes, _) -> if includeTypes then formatWithType p else p.Name
                | FormatParameterOption.Sql -> p.SqlParameterName
            )
        match fpo with
        | FormatParameterOption.Dotnet(_, true) ->
            strs |> Seq.map(fun s -> ", " + s) |> String.concat ""
        | _ -> strs |> String.concat ", "
    let printFnWithParameters(name: string, parameters: NamedType seq, fpo: FormatParameterOption) =
        name + "(" + (formatParameters(parameters, fpo)) + ")"

    let formatToDotnetInput(nt: NamedType) =
        if nt.Nullable then
            if nt.SqlType.IsReferenceType then ($"(match {nt.Name} with | ValueSome x -> x :> (obj | null) | ValueNone -> null)")
            else ($"(match {nt.Name} with | ValueSome x -> Nullable(x) | ValueNone -> Nullable())")
        else nt.Name
    let readParameterCode(nt: NamedType, reader: string, position: int) =
        if nt.Nullable then
            $"(if {reader}.IsDBNull({position}) then ValueNone else ValueSome({SqlType.getterStr(nt.SqlType, reader, position)}))"
        else
            $"{SqlType.getterStr(nt.SqlType, reader, position)}"
    let toSqlParameter(nt: NamedType) =
        match nt.SqlType with
        | SqlType.UserDefinedTableType udttName ->
            $"SqlParameter(\"{nt.SqlParameterName}\", Data.SqlDbType.Structured, TypeName = \"{udttName}\", Value = {udttName}.ToDataTable({formatToDotnetInput nt}))"
        | _ -> // primitive type
            $"SqlParameter(\"{nt.SqlParameterName}\", SqlDbType.{nt.SqlType.SqlDbType.ToString()}, Value = {formatToDotnetInput nt})"

module private Helpers =
    let memberExpr(name: string) = "    member _." + name + " = " + name
    let readSingleRow(nts: NamedType list, outputTypeName: string) =
        let inputs = nts |> Seq.mapi(fun i nt -> NamedType.readParameterCode(nt, "reader", i)) |> String.concat ", "
        $"{outputTypeName}({inputs})"

module private UserDefinedTableT =
    let fSharpTypeDef(udtt: UserDefinedTableT) =
        let readSingleRow =
            udtt.Cols |> List.mapi(fun i col -> NamedType.readParameterCode(col, "reader", i)) |> String.concat ", "
        let writeRow =
            udtt.Cols |> List.map(fun col -> "row." + col.Name) |> String.concat ", "
        [   yield $"type {udtt.Name}({NamedType.formatParameters(udtt.Cols, FormatParameterOption.Dotnet(true, false))}) ="
            for col in udtt.Cols do
                yield $"    member _.{col.Name} = {col.Name}"
            yield $"    static member FromReader(reader: SqlDataReader) ="
            yield $"        let b = ImmutableArray.CreateBuilder<{udtt.Name}>()"
            yield "        while reader.Read() do"
            yield $"            b.Add({udtt.Name}({readSingleRow}))"
            yield "        reader.Close()"
            yield "        b.ToImmutable()"

            yield $"    static member ToDataTable(rows: ImmutableArray<{udtt.Name}>) ="
            yield "        let dt = new DataTable()" // TODO: think about disposal (even though unimportant https://learn.microsoft.com/en-us/answers/questions/224772/do-i-need-to-use-dispose)
            for col in udtt.Cols do
                let (sqlType: SqlType) = col.SqlType
                yield $"""        dt.Columns.Add("{col.Name}", typeof<{SqlType.dotnetTypeStr sqlType}>) |> ignore"""
            yield "        for row in rows do"
            yield $"            dt.Rows.Add({writeRow}) |> ignore"
            yield "        dt"
        ]

module private Command =
    let format(c: Command) =
        let hasParams = not (Seq.isEmpty c.Parameters)
        let inputTypeName = if hasParams then "Input" else "unit"
        let commandParamsExpr =
            if hasParams then "(fun i -> i.CommandParams)"
            else "(fun () -> ImmutableArray.Empty)"

        // ---- Input type (not needed for TableGetter or when there are no parameters) ----
        let inputLines =
            match c.CommandType with
            | CommandTy.TableGetter -> []
            | _ when not hasParams -> []
            | _ ->
                let paramList = c.Parameters |> Seq.toList
                let lastIdx = paramList.Length - 1
                [   yield $"    type Input({NamedType.formatParameters(c.Parameters, FormatParameterOption.Dotnet(true, false))}) ="
                    yield  "        member _.CommandParams ="
                    yield  "            ImmutableArray.Create<SqlParameter>("
                    for i, p in paramList |> List.mapi (fun i p -> i, p) do
                        let comma = if i < lastIdx then "," else ""
                        yield $"                {NamedType.toSqlParameter p}{comma}"
                    yield  "            )"
                ]

        // ---- Output type (Table/Query only) ----
        let outputLines =
            match c.Returns with
            | Return.Table(nts, _) ->
                [   yield $"    type Output({NamedType.formatParameters(nts, FormatParameterOption.Dotnet(true, false))}) ="
                    for nt in nts do
                        yield $"        member _.{nt.Name} = {nt.Name}" ]
            | _ -> []

        // ---- CommandText ----
        let commandTextStr =
            match c.CommandType with
            | CommandTy.StoredProc -> c.QualifiedName
            | CommandTy.UDF ->
                match c.Returns with
                | Return.Single _ -> $"SELECT {c.QualifiedName}({NamedType.formatParameters(c.Parameters, FormatParameterOption.Sql)})"
                | Return.Table _ -> $"SELECT * FROM {c.QualifiedName}({NamedType.formatParameters(c.Parameters, FormatParameterOption.Sql)})"
                | Return.None -> failwith "UDF with no return type"
            | CommandTy.TableGetter -> $"SELECT * FROM {c.QualifiedName}"
            | CommandTy.SqlCommand sql -> sql

        // ---- Command value ----
        let commandLines =
            match c.CommandType, c.Returns with

            | CommandTy.StoredProc, Return.None ->
                [   yield $"    let Command ="
                    yield $"        GenADO.StoredProcNonQuery<{inputTypeName}>(\"{commandTextStr}\", {commandParamsExpr})" ]

            | CommandTy.StoredProc, Return.Table(nts, _) ->
                let readRow = nts |> List.mapi(fun i nt -> NamedType.readParameterCode(nt, "record", i)) |> String.concat ", "
                [   yield $"    let Command ="
                    yield $"        GenADO.StoredProcQuery<{inputTypeName}, Output>("
                    yield $"            \"{commandTextStr}\","
                    yield $"            {commandParamsExpr},"
                    yield $"            (fun record -> Output({readRow})))" ]

            | CommandTy.UDF, Return.Table(nts, _) ->
                let readRow = nts |> List.mapi(fun i nt -> NamedType.readParameterCode(nt, "record", i)) |> String.concat ", "
                [   yield $"    let Command ="
                    yield $"        GenADO.TableUDF<{inputTypeName}, Output>("
                    yield $"            \"{commandTextStr}\","
                    yield $"            {commandParamsExpr},"
                    yield $"            (fun record -> Output({readRow})))" ]

            | CommandTy.UDF, Return.Single(sqlType, nullable) ->
                let outputTypeStr =
                    let t = SqlType.dotnetTypeStr sqlType
                    if nullable then $"{t} voption" else t
                let read = SqlType.getterStr(sqlType, "record", 0)
                let result = if nullable then $"if record.IsDBNull(0) then ValueNone else ValueSome({read})" else read
                [   yield $"    let Command ="
                    yield $"        GenADO.ScalarUDF<{inputTypeName}, {outputTypeStr}>("
                    yield $"            \"{commandTextStr}\","
                    yield $"            {commandParamsExpr},"
                    yield $"            (fun record -> {result}))" ]

            | CommandTy.TableGetter, Return.Table(nts, _) ->
                let readRow = nts |> List.mapi(fun i nt -> NamedType.readParameterCode(nt, "record", i)) |> String.concat ", "
                [   yield $"    let Command ="
                    yield $"        GenADO.TableGetter<Output>(\"{commandTextStr}\", (fun record -> Output({readRow})))" ]

            | CommandTy.SqlCommand _, _ ->
                failwith "SqlCommand code generation is not supported via the new interfaces"

            | combination ->
                failwith $"Unexpected CommandTy/Return combination: {combination}"

        [   yield  "[<RequireQualifiedAccess>]"
            yield  $"module {c.Name} ="
            yield! inputLines
            yield! outputLines
            yield! commandLines ]

let getGeneratedFileLines(designTimeConn: string, designTimeServer: string, generatedNamespace: string) =

    let dbInfo = DbInfo.Get(designTimeConn, designTimeServer)

    let b = ImmutableArray.CreateBuilder<string>()

    let title(str: string) =
        let line = @"// " + String.init str.Length (fun _ -> "-")
        [
            line
            "// " + str
            line
        ] |> b.AddRange

    let opens() =
        b.Add "open System"
        b.Add "open System.Data"
        b.Add "open Microsoft.Data.SqlClient"
        b.Add "open System.Collections.Immutable"
        b.Add "open ADONetCodeGen.Core"

    b.Add @"// This code is auto-generated"

    for schema, schemaInfo in dbInfo do

        do  b.Add $"namespace {generatedNamespace}.{schema}"
            opens()

        if schemaInfo.UserDefinedTableTypes |> List.isEmpty |> not then
            title "User-Defined Table Types"
            for udt in schemaInfo.UserDefinedTableTypes do
                b.AddRange(UserDefinedTableT.fSharpTypeDef udt)

        if schemaInfo.StoredProcedures |> List.isEmpty |> not then
            title "Stored Procedures"
            for tsp in schemaInfo.StoredProcedures do
                let c = Command.FromTypedStoredProcedure(tsp)
                b.AddRange(Command.format c)

        if schemaInfo.UserDefinedFunctions |> List.isEmpty |> not then
            title "User Defined Functions"
            for tudf in schemaInfo.UserDefinedFunctions do
                let c = Command.FromTypedUDF(tudf)
                b.AddRange(Command.format c)

        if schemaInfo.TableGetters |> List.isEmpty |> not then
            title "Table Getters"
            b.Add $"namespace {generatedNamespace}.{schema}.TableGetters"
            opens()
            for tg in schemaInfo.TableGetters do
                let c = Command.GetterFromTypedTable(tg)
                b.AddRange(Command.format c)

    b.ToImmutable()