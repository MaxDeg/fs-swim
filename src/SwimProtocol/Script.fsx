#r "../../packages/FSharpx.Collections/lib/net40/FSharpx.Collections.dll"
#r "../../packages/FSharp.Control.AsyncSeq/lib/net45/FSharp.Control.AsyncSeq.dll"
#r "../../packages/FSharpx.Async/lib/net40/FSharpx.Async.dll"
#r "../../packages/FSharpx.Extras/lib/40/FSharpx.Extras.dll"
#r "../../packages/FSharp.Control.Reactive/lib/net45/FSharp.Control.Reactive.dll"
#r "../../packages/Rx-Core/lib/net45/System.Reactive.Core.dll"
#r "../../packages/Rx-Interfaces/lib/net45/System.Reactive.Interfaces.dll"
#load "../../paket-files/MaxDeg/fs-msgpack/src/MsgPack/DataType.fs"
#load "../../paket-files/MaxDeg/fs-msgpack/src/MsgPack/Value.fs"
#load "../../paket-files/MaxDeg/fs-msgpack/src/MsgPack/Packer.fs"
#load "../../paket-files/MaxDeg/fs-msgpack/src/MsgPack/Unpacker.fs"
#load "Utils.fs"
#load "Types.fs"
#load "Message.fs"
#load "Transport.fs"
#load "Dissemination.fs"
#load "Membership.fs"
#load "FailureDetection.fs"
#load "Swim.fs"

open SwimProtocol

let node1 = Swim.init { Swim.defaultConfig with Port = 1337 } []
let node2 = Swim.init { Swim.defaultConfig with Port = 1338 } [ ("mdg-ubuntu", 1337) ]


open System
open FSharp.Control.Reactive
open FSharpx.Control.Observable
#time

let random = new Random()

type PeriodMember = uint64 * string
type State = { SeqNr: uint64; Members : string list; PingTargets : string list }
type ProtocolResult = | Ack of PeriodMember | Timeout of PeriodMember

let ackEvent = new Event<ProtocolResult>()

let incomingAck =
    let acks =
        ackEvent.Publish
        |> Observable.replayWindow (TimeSpan.FromSeconds 8.)
    
    Observable.connect acks |> ignore    
    acks |> Observable.asObservable

let ackFor seqNr memb = 
    incomingAck
    |> Observable.filter (function Ack(aseqNr, aMemb) -> aseqNr = seqNr && memb = aMemb | _ -> false)

let periodMember state = (state.SeqNr, List.head state.PingTargets)
let periodTimeout state = periodMember state |> Timeout |> Observable.result |> Observable.delay (TimeSpan.FromSeconds 8.)
let pingTimout state = periodMember state |> Timeout |> Observable.result |> Observable.delay (TimeSpan.FromSeconds 5.)

let pingRequest state = 
    printfn "Sending ping request for %i" state.SeqNr

let ping state =
    printfn "Sending ping for %i" state.SeqNr
    //periodMember state |> Ack |> ackEvent.Trigger
    Async.Start(async {
        do! Async.Sleep (random.Next(5, 9) * 1000)
        periodMember state |> Ack |> ackEvent.Trigger
    })
    
let period state =
    let incomingAck = ackFor (state.SeqNr) (List.head state.PingTargets)
    let result =
        incomingAck
        |> Observable.merge (pingTimout state)
        |> Observable.filter (function Timeout _ -> pingRequest state; false | _ -> true)
        |> Observable.amb (periodTimeout state)
        |> Observable.head
    
    ping state     
    Observable.wait result

let state = { SeqNr = 0UL; Members = [ "m1"; "m2" ]; PingTargets = List.shuffle [ "m1"; "m2" ] }

let nextPeriodState state =
    let pingTargets =
        match state.PingTargets with
        | [] | _::[] -> List.shuffle state.Members
        | _::tail -> tail
    
    { state with SeqNr = state.SeqNr + 1UL; PingTargets = pingTargets }

let periodProtocol =
    Observable.generateTimeSpan 
        state (fun _ -> true)
        nextPeriodState id
        (fun _ -> TimeSpan.FromSeconds 8.)

periodProtocol |> Observable.subscribe (fun s -> period s |> printfn "Result %A")


