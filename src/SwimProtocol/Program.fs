open SwimProtocol
open System
open System.Net
open System.Net.Sockets

let randomPort() = 1337
let parseEndpoint (endpoint : string) =
    let parts = endpoint.Split(':')
    parts.[0], int parts.[1]

[<EntryPoint>]
let main argv = 
    printfn "Swim Protocol starting"
    let port = randomPort()

    let local = MemberList.makeLocal port
    let transport = Transport.create port
    let disseminator = EventsDissemination.create()
    let memberList = 
        if Array.isEmpty argv then
            MemberList.create disseminator
        else
            let h, p = argv.[0] |> parseEndpoint
            MemberList.createWith disseminator [ MemberList.makeMember h p ]

    FailureDetection.init { Socket = transport
                            Local = local
                            MemberList = memberList
                            PeriodTimeout = TimeSpan.FromSeconds 2.0
                            PingTimeout = TimeSpan.FromMilliseconds 100.0
                            PingRequestGroupSize = 3 }
    
    printfn "Swim Protocol started"
    Console.ReadKey() |> ignore
    0 // return an integer exit code
