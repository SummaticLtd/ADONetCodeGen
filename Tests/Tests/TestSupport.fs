namespace Tests

open System
open System.IO

/// Minimal assertion helpers (a trimmed-down version of the SummaticApp TestUtils.Assert).
type Assert =
    [<Diagnostics.DebuggerHidden>]
    static member Equal<'a when 'a: equality and 'a: not null>(expected: 'a, actual: 'a, ?errorMsg: string) =
        if expected <> actual then
            let suffix = match errorMsg with None -> "" | Some msg -> Environment.NewLine + msg
            failwith (
                "Expected: " + expected.ToString() + Environment.NewLine
                + "But was: " + actual.ToString() + suffix
            )

    static member True(condition: bool, ?errorMsg: string) =
        if not condition then
            match errorMsg with
            | None -> failwith "Expected condition to be true but was false."
            | Some em -> failwith ("Expected condition to be true but was false." + Environment.NewLine + em)

    static member Fail(errorMsg: string) : unit = failwith errorMsg

module Locations =

    /// Walks up from the test assembly until it finds the directory containing ADONetCodeGen.slnx.
    let SolutionDirectory =
        let mutable currDir = DirectoryInfo(AppContext.BaseDirectory)
        while not (isNull (box currDir)) && not (File.Exists(Path.Combine(currDir.FullName, "ADONetCodeGen.slnx"))) do
            currDir <- currDir.Parent
        if isNull (box currDir) then
            failwith "Could not locate the solution directory (ADONetCodeGen.slnx) above the test assembly."
        currDir
