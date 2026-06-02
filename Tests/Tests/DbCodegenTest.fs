module Tests.DbCodegenTest

open System
open System.IO
open System.Text
open System.Collections.Immutable
open SimpleTests
open ADONetCodeGen.Writer
open GeneratedTestConnection.DataCore

let [<Literal>] private hashText = "// Sql Hash: "

/// Hash of all the SQL source: every *.sql file plus the SQL project file,
/// sorted by path, concatenating each file's name and lines (MurmurHash128).
let private computeSqlHash (sqlDir: DirectoryInfo) =
    let sqlFileInfos =
        let sqlProjFile = sqlDir.EnumerateFiles("*.sqlproj") |> Seq.exactlyOne
        let b = ImmutableArray.CreateBuilder<FileInfo>()
        sqlDir.EnumerateFiles("*.sql", SearchOption.TopDirectoryOnly) |> b.AddRange
        let folders = sqlDir.EnumerateDirectories() |> Seq.filter (fun di -> di.Name <> "bin" && di.Name <> "obj")
        for folder in folders do
            folder.EnumerateFiles("*.sql", SearchOption.AllDirectories) |> b.AddRange
        let sqlFiles = b.ToImmutable().Sort(fun a b -> String.CompareOrdinal(a.FullName, b.FullName))
        sqlFiles.Add(sqlProjFile)
    let sb = StringBuilder()
    for fi in sqlFileInfos do
        sb.Append(fi.Name) |> ignore
        for line in File.ReadAllLines(fi.FullName) do
            sb.Append(line) |> ignore
    let strBytes = Encoding.UTF8.GetBytes(sb.ToString())
    use hashfn = Murmur.MurmurHash.Create128()
    let bytes = hashfn.ComputeHash(strBytes)
    let uIntHash = BitConverter.ToUInt64(bytes, 0)
    sqlFileInfos, uIntHash.ToString("x")

let private adoNetCodeIsUpToDate =
    Test.Sync("adoNetCodeIsUpToDate", fun () ->
        let sqlDir = DirectoryInfo(Path.Combine(Locations.SolutionDirectory.FullName, "Tests", "TestSql"))
        let sqlFileInfos, sqlHash = computeSqlHash sqlDir
        let path =
            Path.Combine(Locations.SolutionDirectory.FullName, "Tests", "GeneratedTestConnection", "Generated.fs")

        let fileNeedsUpdating =
            if File.Exists(path) then
                let text = File.ReadAllLines(path)
                if text.Length = 0 then true
                else
                    let firstLine = text.[0]
                    firstLine.Length < hashText.Length || firstLine.[hashText.Length ..] <> sqlHash
            else true

        if fileNeedsUpdating then
            // The generated code is out of date. Confirm the database has been deployed and is current
            // (dacpac newer than the newest .sql file) before regenerating.
            let dbIsDeployed =
                let latestSqlFileInfo = sqlFileInfos |> Seq.maxBy (fun fi -> fi.LastWriteTimeUtc)
                let dacPacs = sqlDir.EnumerateFiles("*.dacpac", SearchOption.AllDirectories).ToImmutableArray()
                if dacPacs.Length = 0 then
                    Error "ADO.NET code needs updating, but the database has not been deployed: there is no dacpac file. Build TestSql to resolve this."
                else
                    let lastDacpacWriteTime = dacPacs |> Seq.map (fun fi -> fi.LastWriteTimeUtc) |> Seq.max
                    if lastDacpacWriteTime < latestSqlFileInfo.LastWriteTimeUtc then
                        Error "ADO.NET code needs updating, but the database has not been deployed: the dacpac file is out of date. Build TestSql to resolve this."
                    else
                        Ok ()
            match dbIsDeployed with
            | Error msg -> Assert.Fail msg
            | Ok () ->
                let firstLine = hashText + sqlHash
                let lines = getGeneratedFileLines(designTimeConn, designTimeServer, "GeneratedADONET")
                File.WriteAllLines(path, ImmutableArray.Create(firstLine).AddRange(lines), Encoding.UTF8)
                Assert.Fail("ADO.NET code was out of date. It has now been updated. Commit the change and re-run.")
    )

let DbCodegenTestList =
    TestList("DbCodegenTest", [ adoNetCodeIsUpToDate ])
