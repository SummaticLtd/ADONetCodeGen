module Tests.EntryPoint

open SimpleTests

let testFolders = [
    TestFolder("Tests.DbCodegenTest", [ Tests.DbCodegenTest.DbCodegenTestList ])
    TestFolder("Tests.UdfTest", [ Tests.UdfTest.UdfTestList ])
]

[<EntryPoint>]
let main (args: string array) : int =
    Runner.Run(args, testFolders)
