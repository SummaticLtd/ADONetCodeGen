module Tests.EntryPoint

open SimpleTests

let testFolders = [
    TestFolder("Tests.DbCodegenTest", [ Tests.DbCodegenTest.DbCodegenTestList ])
    TestFolder("Tests.UdfTest", [ Tests.UdfTest.UdfTestList ])
    TestFolder("Tests.ScalarTypeTests", [ Tests.ScalarTypeTests.ScalarTypeTestList ])
    TestFolder("Tests.TableUdfTests", [ Tests.TableUdfTests.TableUdfTestList ])
    TestFolder("Tests.StoredProcTests", [ Tests.StoredProcTests.StoredProcTestList ])
    TestFolder("Tests.BatchTests", [ Tests.BatchTests.BatchTestList ])
]

[<EntryPoint>]
let main (args: string array) : int =
    Runner.Run(args, testFolders)
