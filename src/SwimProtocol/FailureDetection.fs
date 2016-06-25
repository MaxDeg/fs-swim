module SwimProtocol.FailureDetection

open System
open System.Net
open Transport
open Membership
open FSharpx.Option
open FSharp.Control
open FSharp.Control.Reactive
open FSharpx.Control.Observable

type private State =
    { SeqNumber : PeriodSeqNumber
      Local : Member
      MemberList : MemberList
      IncomingAck : IObservable<Ack>
      TriggerMessage : IPEndPoint * Message -> unit
      PingTargets : (Member * IncarnationNumber) list
      PeriodTimeout : TimeSpan
      PingTimeout : TimeSpan
      PingRequestGroupSize : int }

type private PingStatus = PingAcked | PingTimeout of PeriodSeqNumber * Member

let private ackFor seqNr memb { IncomingAck = incomingAck } =
    incomingAck |> Observable.filter (fun (ackSeqNr, ackMemb) -> seqNr = ackSeqNr && memb = ackMemb)
                |> Observable.map (fun a -> PingAcked)

let private pingTimeoutFor seqNr memb { PingTimeout = timeout } =
    PingTimeout(seqNr, memb) |> Observable.single |> Observable.delay timeout
    
let private periodTimeoutFor seqNr memb { PeriodTimeout = timeout } =
    PingTimeout(seqNr, memb) |> Observable.single |> Observable.delay timeout

let private ping seqNr memb { TriggerMessage = trigger } =
    trigger(memb.Address, PingMessage seqNr)
    
let private pingRequest seqNr memb state =
    let members = state.MemberList.Members() |> List.shuffle
                                             |> List.choose (fun (m, _) -> if m <> memb then Some m else None)

    members |> List.take (Math.Min(state.PingRequestGroupSize, List.length members))
            |> List.iter (fun m -> state.TriggerMessage(m.Address, PingRequestMessage(seqNr, memb)))
    

let private ackPing seqNr target { Local = local; TriggerMessage = trigger } =
     trigger(target, AckMessage(seqNr,local))
    
let private ackPingRequest seqNr memb target state =
    let pingResult =
        ackFor seqNr memb state
        |> Observable.merge (pingTimeoutFor seqNr memb state)
        |> Observable.head
        
    ping seqNr memb state
    match Observable.wait pingResult with
    | PingAcked -> state.TriggerMessage(target, AckMessage(seqNr,memb))
    | _ -> ()

let private runPeriod ({ SeqNumber = seqNr; PingTargets = pingTargets } as state) =
    maybe {
        let! memb, incarnation = List.tryHead pingTargets
        let pingResult =
            ackFor seqNr memb state
            |> Observable.merge (pingTimeoutFor seqNr memb state)
            |> Observable.perform (function PingTimeout(seqNr, memb) -> pingRequest seqNr memb state | _ -> ()) 
            |> Observable.filter (function PingAcked _ -> true | PingTimeout _ -> false)
            |> Observable.amb (periodTimeoutFor seqNr memb state)
            |> Observable.head
        
        ping seqNr memb state
        match Observable.wait pingResult with
        | PingAcked ->
            printfn "Ping %A successfully" seqNr
            state.MemberList.Alive memb incarnation
        | PingTimeout _ ->
            printfn "Failed to ping %A" seqNr
            state.MemberList.Suspect memb incarnation
    } |> ignore


let private nextState state =
    let pingTargets =
        match state.PingTargets with
        | [] | _::[] -> state.MemberList.Members() |> List.shuffle
        | _::tail -> tail
    
    { state with SeqNumber = state.SeqNumber + 1UL; PingTargets = pingTargets }

let private handle state (addr, msg) =
    match msg with
    | PingMessage seqNr -> ackPing seqNr addr state
    | PingRequestMessage(seqNr, memb) -> ackPingRequest seqNr memb addr state
    | _ -> ()

type Config =
    { Local : Member
      MemberList : MemberList
      PeriodTimeout : TimeSpan
      PingTimeout : TimeSpan
      PingRequestGroupSize : int }

let run config incomingMessages =
    let filterAck = function _, AckMessage ack -> Some ack | _ -> None
    let messageEvent = new Event<IPEndPoint * Message>()
    let incomingAck = incomingMessages |> Observable.choose filterAck
                                       |> Observable.replayWindow config.PeriodTimeout
    
    let state =
        { SeqNumber = 0UL
          Local = config.Local
          MemberList = config.MemberList
          IncomingAck = incomingAck
          TriggerMessage = messageEvent.Trigger
          PingTargets = config.MemberList.Members() |> List.shuffle
          PeriodTimeout = config.PeriodTimeout
          PingTimeout = config.PingTimeout
          PingRequestGroupSize = config.PingRequestGroupSize }

    printfn "Failure detection system is running"
    Observable.connect incomingAck |> ignore

    Observable.generateTimeSpan
        state (fun _ -> true)
        nextState id
        (fun _ -> config.PeriodTimeout)
    |> Observable.subscribe runPeriod
    |> ignore
      
    incomingMessages |> Observable.subscribe (handle state)
                     |> ignore
    
    messageEvent.Publish
