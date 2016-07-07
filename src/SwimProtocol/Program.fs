open SwimProtocol
open System

let parseEndpoint (endpoint : string) =
    let parts = endpoint.Split(':')
    parts.[0], int parts.[1]

[<EntryPoint>]
let main argv =
//    let port = Udp.randomPort()
//
//    argv
//    |> Array.map parseEndpoint
//    |> Array.toList
//    |> Swim.init { Swim.defaultConfig with Port = port }

    let localName = System.Net.Dns.GetHostName()

    let __ = Swim.start { Swim.defaultConfig with Port = 1337us; PeriodTimeout = TimeSpan.FromSeconds(1.) } []
    let __ = Swim.start { Swim.defaultConfig with Port = 1338us; PeriodTimeout = TimeSpan.FromSeconds(1.) } []//[ (localName, 1337us) ]
    //let __ = Swim.start { Swim.defaultConfig with Port = 1339us; PeriodTimeout = TimeSpan.FromSeconds(1.) } [ (localName, 1337us) ]
    
    Console.ReadKey() |> ignore
    0 // return an integer exit code
