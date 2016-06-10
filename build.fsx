// include Fake libs
#r "./packages/FAKE/tools/FakeLib.dll"

open Fake

// Directories
let buildDir  = "./build/"
let deployDir = "./deploy/"


// Filesets
let appReferences  =
    !! "/src/**/*.csproj"
      ++ "/src/**/*.fsproj"

// version info
let version = "0.1"  // or retrieve from CI server

let build() =
    // compile all projects below src/app/
    MSBuildDebug buildDir "Build" appReferences
        |> Log "AppBuild-Output: "

// Targets
Target "Clean" (fun _ ->
    CleanDirs [buildDir; deployDir]
)

Target "Build" build

Target "Deploy" (fun _ ->
    !! (buildDir + "/**/*.*")
        -- "*.zip"
        |> Zip buildDir (deployDir + "ApplicationName." + version + ".zip")
)

Target "Watch" (fun _ ->
    use watcher = !! "**/*.fs" |> WatchChanges (fun changes -> 
        tracefn "%A" changes
        build()
    )

    System.Console.ReadLine() |> ignore //Needed to keep FAKE from exiting
    watcher.Dispose() // Use to stop the watch from elsewhere, ie another task.
)

// Build order
"Clean"
  ==> "Build"
  ==> "Deploy"

// start build
RunTargetOrDefault "Build"
