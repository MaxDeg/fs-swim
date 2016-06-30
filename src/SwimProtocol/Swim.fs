module SwimProtocol.Swim

open System
open Membership
open Transport
open FailureDetection
open FSharp.Control
open FSharp.Control.Reactive
open FSharpx.Control.Observable

type Host = string * int

let defaultConfig =
    { Port = 1337us
      PeriodTimeout = TimeSpan.FromSeconds 2.
      PingTimeout = TimeSpan.FromMilliseconds 300.
      PingRequestGroupSize = 3
      SuspectTimeout = TimeSpan.FromSeconds 2. }

let private decodeMessage (addr, bytes) =
    Message.decode bytes
    |> Option.map (fun (msg, events) -> (addr, msg), events)

let init ({ Port = port } as config : Config) hosts =
    let local = MemberList.makeLocal port
    let disseminator = Dissemination.create()
    let udp = Udp.connect (int port)

    printfn "Starting Swim protocol on %O" local
    
    let memberList =
        hosts |> List.map (fun (h, p) -> MemberList.makeMember h p)
              |> MemberList.createWith disseminator config local

    let encodeMessage msg =
        let encodedMsg = Message.encodeMessage msg
        let events = disseminator.Pull (memberList.Length) (Udp.maxSize - Message.sizeOf encodedMsg)

        Message.encode encodedMsg events

    let messageReceived = udp.Received |> Observable.choose decodeMessage
    
    let diss =
        messageReceived |> Observable.map snd
                        |> Observable.subscribe (
                            List.choose (function MembershipEvent e -> Some e | _ -> None)
                            >> List.iter (function
                                            | Alive(n, inc) -> memberList.Alive n inc
                                            | Suspect(n, inc) -> memberList.Suspect n inc
                                            | Dead(n, inc) -> memberList.Dead n inc))

    let failureDetection =
        messageReceived |> Observable.map fst
                        |> FailureDetection.run config local memberList
                        |> Observable.map (fun (addr, msg) -> addr, encodeMessage msg)
                        |> Observable.subscribe udp.Send

    { new IDisposable with
        member __.Dispose() =
            diss.Dispose()
            failureDetection.Dispose() }    
