open SwimProtocol
open Transport
open System

let parseEndpoint (endpoint : string) =
    let parts = endpoint.Split(':')
    parts.[0], int parts.[1]

[<EntryPoint>]
let main argv = 
    printfn "Swim Protocol starting"
//    let port = Udp.randomPort()
//
//    argv
//    |> Array.map parseEndpoint
//    |> Array.toList
//    |> Swim.init { Swim.defaultConfig with Port = port }

    let localName = System.Net.Dns.GetHostName()

    let node1 = Swim.init { Swim.defaultConfig with Port = 1337us } []
    let node2 = Swim.init { Swim.defaultConfig with Port = 1338us } [ (localName, 1337us) ]
    
    printfn "Swim Protocol started"
    Console.ReadKey() |> ignore
    0 // return an integer exit code
