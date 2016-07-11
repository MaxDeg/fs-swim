open SwimProtocol
open System

let parseEndpoint (endpoint : string) =
    let parts = endpoint.Split(':')
    parts.[0], uint16 parts.[1]

[<EntryPoint>]
let main argv =
    let port, skipCount = 
        if Array.length argv > 0 then uint16 argv.[0], 1
        else Udp.randomPort(), 0
    
    let swim =
        argv
        |> Array.skip skipCount
        |> Array.map parseEndpoint
        |> Array.toList
        |> Swim.start { Swim.defaultConfig with Port = port }

    let showState () =
        let showDelimiter() =
            printfn "+----------------------------+-------------+"
        let showLine = printfn "| %-26O | %-11O |"

        let showHeader() =
            showDelimiter()
            showLine "Current Node" (port.ToString())
            showDelimiter()
            showLine "Members" "Incarnation"
            showDelimiter()

        Console.Clear()
        showHeader()
        Swim.members swim |> List.iter (fun (m, i) -> showLine (m.ToString()) (i.ToString()))
        showDelimiter()
        Swim.events swim |> List.iter (printfn "# %A")

    Async.Start(async {
        while true do
            showState()
            do! Async.Sleep(2000)
    })
    
//    let localName = System.Net.Dns.GetHostName()

//    let __ = Swim.start { Swim.defaultConfig with Port = 1337us } []
//    let __ = Swim.start { Swim.defaultConfig with Port = 1338us } [ (localName, 1337us) ]
//    let __ = Swim.start { Swim.defaultConfig with Port = 1339us } [ (localName, 1337us) ]
    
    Console.ReadKey() |> ignore
    Swim.stop swim
    Console.ReadKey() |> ignore
    0 // return an integer exit code
