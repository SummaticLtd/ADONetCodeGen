namespace Tests

open System
open System.IO

module Locations =

    /// Walks up from the test assembly until it finds the directory containing ADONetCodeGen.slnx.
    let SolutionDirectory =
        let mutable currDir = DirectoryInfo(AppContext.BaseDirectory)
        while not (isNull (box currDir)) && not (File.Exists(Path.Combine(currDir.FullName, "ADONetCodeGen.slnx"))) do
            currDir <- currDir.Parent
        if isNull (box currDir) then
            failwith "Could not locate the solution directory (ADONetCodeGen.slnx) above the test assembly."
        currDir
