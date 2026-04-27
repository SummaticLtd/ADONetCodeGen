namespace ADONetCodeGen.Core

open Microsoft.Data.SqlClient
open System
open System.Data
open System.Collections.Immutable

/// Describes an SqlConnection with optional SqlTransaction for passing into an SqlCommand.
/// This type ensures that at most one transaction can be active at a time,
/// and allows the handling of transactions to be automatic.
type ISqlConnection =
    abstract Connection: SqlConnection
    abstract Transaction: SqlTransaction option

type SqlConnWithTransaction(conn: SqlConnection, tran: SqlTransaction) =
    member _.CommitAsync() = tran.CommitAsync()
    member _.RollbackAsync() = tran.RollbackAsync()
    interface ISqlConnection with
        member _.Connection = conn
        member _.Transaction = Some tran
    interface IDisposable with
        member _.Dispose() =
            tran.Dispose()
            conn.Dispose()

type SqlConn(conn: SqlConnection) =
    interface ISqlConnection with
        member _.Connection = conn
        member _.Transaction = None
    interface IDisposable with member _.Dispose() = conn.Dispose()

module private ImmArray =
    let inline map<'a, 'b> ([<InlineIfLambda>] f:'a -> 'b) (arr:ImmutableArray<'a>) =
        let builder = ImmutableArray.CreateBuilder<'b>(arr.Length)
        for i = 0 to arr.Length - 1 do
            builder.Add(f(arr.[i]))
        builder.MoveToImmutable()
    let singleton<'a>(x:'a) = ImmutableArray.Create(x)
    let inline collect<'T, 'U> ([<InlineIfLambda>] mapping: 'T -> ImmutableArray<'U>) (l:ImmutableArray<'T>) =
        let b = ImmutableArray.CreateBuilder<'U>()
        for ind = 0 to l.Length - 1 do
            b.AddRange(mapping(l.[ind]))
        b.ToImmutable()

// ---------------------------------------------------------------
// Classes implemented by generated code to describe stored procedures, UDFs, and table getters
// ---------------------------------------------------------------

[<RequireQualifiedAccess>]
module GenADO =

    /// A stored procedure that performs work and returns no rows (no SELECT output).
    type StoredProcNonQuery<'Inputs>(commandText: string, commandParams: 'Inputs -> ImmutableArray<SqlParameter>) =
        member _.CommandText = commandText
        /// Produces the SqlParameters for a given set of inputs
        member _.CommandParams(input: 'Inputs) = commandParams input

    /// A stored procedure that returns zero or more rows, each read via ReadDataRow.
    type StoredProcQuery<'Inputs, 'Output>(commandText: string, commandParams: 'Inputs -> ImmutableArray<SqlParameter>, readDataRow: IDataRecord -> 'Output) =
        member _.CommandText = commandText
        /// Produces the SqlParameters for a given set of inputs
        member _.CommandParams(input: 'Inputs) = commandParams input
        /// Reads a single row from the result set
        member _.ReadDataRow(record: IDataRecord) = readDataRow record

    /// A table-valued UDF that returns zero or more rows, each read via ReadDataRow.
    type TableUDF<'Inputs, 'Output>(commandText: string, commandParams: 'Inputs -> ImmutableArray<SqlParameter>, readDataRow: IDataRecord -> 'Output) =
        member _.CommandText = commandText
        /// Produces the SqlParameters for a given set of inputs
        member _.CommandParams(input: 'Inputs) = commandParams input
        /// Reads a single row from the result set
        member _.ReadDataRow(record: IDataRecord) = readDataRow record

    /// A scalar UDF that returns a single value (e.g. int voption, bool voption).
    type ScalarUDF<'Inputs, 'Output>(commandText: string, commandParams: 'Inputs -> ImmutableArray<SqlParameter>, readValue: IDataRecord -> 'Output) =
        member _.CommandText = commandText
        /// Produces the SqlParameters for a given set of inputs
        member _.CommandParams(input: 'Inputs) = commandParams input
        /// Reads the single value from the single-row, single-column result set
        member _.ReadValue(record: IDataRecord) = readValue record

    /// A table getter (SELECT * FROM table) that returns zero or more rows, each read via ReadDataRow.
    /// Has no inputs.
    type TableGetter<'Output>(commandText: string, readDataRow: IDataRecord -> 'Output) =
        member _.CommandText = commandText
        /// Reads a single row from the result set
        member _.ReadDataRow(record: IDataRecord) = readDataRow record

// ---------------------------------------------------------------
// IBatchComponent — the unit of composition
// ---------------------------------------------------------------

/// A component that can be added to an SqlBatch.
/// Owns Commands.Length consecutive result sets in the reader.
/// Contract: ReadData must call NextResult() exactly once per owned result set,
/// leaving the reader fully advanced past all of them before returning.
type BatchComponent<'Output>(commands: ImmutableArray<string * CommandType * ImmutableArray<SqlParameter>>, readData: SqlDataReader -> 'Output) =
    member _.Commands = commands
    member _.ReadData(reader: SqlDataReader) = readData reader

// ---------------------------------------------------------------
// Batch components
// ---------------------------------------------------------------

[<RequireQualifiedAccess>]
module Batch =
    /// Methods for wrapping a single command invocation as a batch component.
    [<RequireQualifiedAccess>]
    module Single =
        let nonQuery<'Inputs>(cmd: GenADO.StoredProcNonQuery<'Inputs>, input: 'Inputs) =
            BatchComponent<unit>(
                ImmArray.singleton (cmd.CommandText, CommandType.StoredProcedure, cmd.CommandParams input),
                fun reader -> reader.NextResult() |> ignore
            )

        let query<'Inputs, 'Output>(cmd: GenADO.StoredProcQuery<'Inputs, 'Output>, input: 'Inputs) =
            BatchComponent<ImmutableArray<'Output>>(
                ImmArray.singleton (cmd.CommandText, CommandType.StoredProcedure, cmd.CommandParams input),
                fun reader ->
                    let b = ImmutableArray.CreateBuilder<'Output>()
                    while reader.Read() do
                        b.Add(cmd.ReadDataRow reader)
                    reader.NextResult() |> ignore
                    b.ToImmutable()
            )

        let tableUDF<'Inputs, 'Output>(cmd: GenADO.TableUDF<'Inputs, 'Output>, input: 'Inputs) =
            BatchComponent<ImmutableArray<'Output>>(
                ImmArray.singleton (cmd.CommandText, CommandType.Text, cmd.CommandParams input),
                fun reader ->
                    let b = ImmutableArray.CreateBuilder<'Output>()
                    while reader.Read() do
                        b.Add(cmd.ReadDataRow reader)
                    reader.NextResult() |> ignore
                    b.ToImmutable()
            )

        let scalarUDF<'Inputs, 'Output>(cmd: GenADO.ScalarUDF<'Inputs, 'Output>, input: 'Inputs) =
            BatchComponent<'Output>(
                ImmArray.singleton (cmd.CommandText, CommandType.Text, cmd.CommandParams input),
                fun reader ->
                    reader.Read() |> ignore
                    let value = cmd.ReadValue reader
                    reader.NextResult() |> ignore
                    value
            )
    /// Two components combined into one batch
    let pair<'A, 'B>(a: BatchComponent<'A>, b: BatchComponent<'B>) =
        BatchComponent<'A * 'B>(
            a.Commands.AddRange(b.Commands),
            fun reader ->
                let resultA = a.ReadData reader
                let resultB = b.ReadData reader
                resultA, resultB
        )
    /// Combines multiple unit-returning components into a single batch component.
    let combineNonQueries(components: ImmutableArray<BatchComponent<unit>>) =
        BatchComponent<unit>(
            (components |> ImmArray.collect (fun c -> c.Commands)),
            fun reader ->
                for c in components do
                    c.ReadData reader
        )
    /// Methods for applying the same command to many inputs as a single batch component.
    [<RequireQualifiedAccess>]
    module Homogeneous =
        let storedProcNonQuery<'Inputs>(cmd: GenADO.StoredProcNonQuery<'Inputs>, data: ImmutableArray<'Inputs>) =
            BatchComponent<unit>(
                (data |> ImmArray.map (fun input -> cmd.CommandText, CommandType.StoredProcedure, cmd.CommandParams input)),
                fun reader ->
                    for _ = 0 to data.Length - 1 do
                        reader.NextResult() |> ignore
            )

        let storedProcQuery<'Inputs, 'Output>(cmd: GenADO.StoredProcQuery<'Inputs, 'Output>, data: ImmutableArray<'Inputs>) =
            BatchComponent<ImmutableArray<ImmutableArray<'Output>>>(
                (data |> ImmArray.map (fun input -> cmd.CommandText, CommandType.StoredProcedure, cmd.CommandParams input)),
                fun reader ->
                    let results = ImmutableArray.CreateBuilder(data.Length)
                    for _ = 0 to data.Length - 1 do
                        let b = ImmutableArray.CreateBuilder<'Output>()
                        while reader.Read() do
                            b.Add(cmd.ReadDataRow reader)
                        results.Add(b.ToImmutable())
                        reader.NextResult() |> ignore
                    results.MoveToImmutable()
            )

        let tableUDF<'Inputs, 'Output>(cmd: GenADO.TableUDF<'Inputs, 'Output>, data: ImmutableArray<'Inputs>) =
            BatchComponent<ImmutableArray<ImmutableArray<'Output>>>(
                (data |> ImmArray.map (fun input -> cmd.CommandText, CommandType.Text, cmd.CommandParams input)),
                fun reader ->
                    let results = ImmutableArray.CreateBuilder(data.Length)
                    for _ = 0 to data.Length - 1 do
                        let b = ImmutableArray.CreateBuilder<'Output>()
                        while reader.Read() do
                            b.Add(cmd.ReadDataRow reader)
                        results.Add(b.ToImmutable())
                        reader.NextResult() |> ignore
                    results.MoveToImmutable()
            )

        let scalarUDF<'Inputs, 'Output>(cmd: GenADO.ScalarUDF<'Inputs, 'Output>, data: ImmutableArray<'Inputs>) =
            BatchComponent<ImmutableArray<'Output>>(
                (data |> ImmArray.map (fun input -> cmd.CommandText, CommandType.Text, cmd.CommandParams input)),
                fun reader ->
                    let results = ImmutableArray.CreateBuilder(data.Length)
                    for _ = 0 to data.Length - 1 do
                        reader.Read() |> ignore
                        results.Add(cmd.ReadValue reader)
                        reader.NextResult() |> ignore
                    results.MoveToImmutable()
            )

// ---------------------------------------------------------------
// Execution module
// ---------------------------------------------------------------

module Command =

    let private mkCmd(conn: ISqlConnection, text: string, cmdType: CommandType, parameters: ImmutableArray<SqlParameter>) =
        let cmd = new SqlCommand(text, conn.Connection, CommandType = cmdType)
        conn.Transaction |> Option.iter (fun t -> cmd.Transaction <- t)
        for p in parameters do
            cmd.Parameters.Add(p) |> ignore
        cmd

    /// Executes a non-query stored procedure and returns the number of records affected.
    let executeStoredProcNonQuery<'Inputs>(conn: ISqlConnection, cmd: GenADO.StoredProcNonQuery<'Inputs>, input: 'Inputs) : Threading.Tasks.Task<int> =
        task {
            use command = mkCmd(conn, cmd.CommandText, CommandType.StoredProcedure, cmd.CommandParams input)
            return! command.ExecuteNonQueryAsync()
        }

    let executeStoredProcQuery<'Inputs, 'Output>(conn: ISqlConnection, cmd: GenADO.StoredProcQuery<'Inputs, 'Output>, input: 'Inputs) : Threading.Tasks.Task<ImmutableArray<'Output>> =
        task {
            use command = mkCmd(conn, cmd.CommandText, CommandType.StoredProcedure, cmd.CommandParams input)
            use! reader = command.ExecuteReaderAsync()
            let b = ImmutableArray.CreateBuilder<'Output>()
            while reader.Read() do
                b.Add(cmd.ReadDataRow reader)
            return b.ToImmutable()
        }

    let executeTableUDF<'Inputs, 'Output>(conn: ISqlConnection, cmd: GenADO.TableUDF<'Inputs, 'Output>, input: 'Inputs) : Threading.Tasks.Task<ImmutableArray<'Output>> =
        task {
            use command = mkCmd(conn, cmd.CommandText, CommandType.Text, cmd.CommandParams input)
            use! reader = command.ExecuteReaderAsync()
            let b = ImmutableArray.CreateBuilder<'Output>()
            while reader.Read() do
                b.Add(cmd.ReadDataRow reader)
            return b.ToImmutable()
        }

    let executeScalarUDF<'Inputs, 'Output>(conn: ISqlConnection, cmd: GenADO.ScalarUDF<'Inputs, 'Output>, input: 'Inputs) : Threading.Tasks.Task<'Output> =
        task {
            use command = mkCmd(conn, cmd.CommandText, CommandType.Text, cmd.CommandParams input)
            use! reader = command.ExecuteReaderAsync()
            reader.Read() |> ignore
            return cmd.ReadValue reader
        }

    let executeTableGetter<'Output>(conn: ISqlConnection, cmd: GenADO.TableGetter<'Output>) : Threading.Tasks.Task<ImmutableArray<'Output>> =
        task {
            use command = mkCmd(conn, cmd.CommandText, CommandType.Text, ImmutableArray.Empty)
            use! reader = command.ExecuteReaderAsync()
            let b = ImmutableArray.CreateBuilder<'Output>()
            while reader.Read() do
                b.Add(cmd.ReadDataRow reader)
            return b.ToImmutable()
        }

    let executeBatch<'T>(conn: ISqlConnection, comp: BatchComponent<'T>) : Threading.Tasks.Task<'T> =
        task {
            use batch = new SqlBatch(conn.Connection)
            conn.Transaction |> Option.iter(fun t -> batch.Transaction <- t)
            for (text, cmdType, parameters) in comp.Commands do
                let cmd = SqlBatchCommand(text, cmdType)
                for p in parameters do
                    cmd.Parameters.Add(p) |> ignore
                batch.BatchCommands.Add(cmd)
            use! reader = batch.ExecuteReaderAsync()
            return comp.ReadData reader
        }

// ---------------------------------------------------------------
// Extension methods
// ---------------------------------------------------------------

[<AutoOpen>]
module Extensions =

    type GenADO.StoredProcNonQuery<'Inputs> with
        member cmd.Execute(conn: ISqlConnection, input: 'Inputs) = Command.executeStoredProcNonQuery(conn, cmd, input)
        member cmd.AsBatchSingle(input: 'Inputs) = Batch.Single.nonQuery(cmd, input)
        member cmd.AsBatchHomogeneous(data: ImmutableArray<'Inputs>) = Batch.Homogeneous.storedProcNonQuery(cmd, data)

    type GenADO.StoredProcQuery<'Inputs, 'Output> with
        member cmd.Execute(conn: ISqlConnection, input: 'Inputs) = Command.executeStoredProcQuery(conn, cmd, input)
        member cmd.AsBatchSingle(input: 'Inputs) = Batch.Single.query(cmd, input)
        member cmd.AsBatchHomogeneous(data: ImmutableArray<'Inputs>) = Batch.Homogeneous.storedProcQuery(cmd, data)

    type GenADO.TableUDF<'Inputs, 'Output> with
        member cmd.Execute(conn: ISqlConnection, input: 'Inputs) = Command.executeTableUDF(conn, cmd, input)
        member cmd.AsBatchSingle(input: 'Inputs) = Batch.Single.tableUDF(cmd, input)
        member cmd.AsBatchHomogeneous(data: ImmutableArray<'Inputs>) = Batch.Homogeneous.tableUDF(cmd, data)

    type GenADO.ScalarUDF<'Inputs, 'Output> with
        member cmd.Execute(conn: ISqlConnection, input: 'Inputs) = Command.executeScalarUDF(conn, cmd, input)
        member cmd.AsBatchSingle(input: 'Inputs) = Batch.Single.scalarUDF(cmd, input)
        member cmd.AsBatchHomogeneous(data: ImmutableArray<'Inputs>) = Batch.Homogeneous.scalarUDF(cmd, data)

    type GenADO.TableGetter<'Output> with
        member cmd.Execute(conn: ISqlConnection) = Command.executeTableGetter(conn, cmd)