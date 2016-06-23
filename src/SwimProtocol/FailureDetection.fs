module SwimProtocol.FailureDetection

open System
open System.Net
open Message
open Transport
open Membership
open FSharp.Control
open FSharp.Control.Reactive
open FSharpx.Control.Observable

type private State =
    { Transport: Transport
      Local : Member
      MemberList : MemberList
      PingTargets : (Member * IncarnationNumber) list
      PeriodTimeout : TimeSpan
      PingTimeout : TimeSpan
      PingRequestGroupSize : int }
    with
        member x.PickPingTarget() =
            match x.PingTargets with
            | [] -> { x with PingTargets = x.MemberList.Members()
                                            |> List.shuffle }
                      .PickPingTarget()
            | head::tail -> head, { x with PingTargets = tail }

        member x.WaitForAck seqNr memb (timeout : TimeSpan) = async {
            let filterAck (addr, msg) =
                match msg with
                | Some(AckMessage(ackSeqNr, ackMemb)) as ack when seqNr = ackSeqNr && memb = ackMemb -> true
                | _ -> false
            
            let! ack = x.Transport.Received
                        |> Observable.map (fun (addr, bytes) -> addr, Message.decode bytes)
                        |> Observable.filter filterAck
                        |> Observable.map Some
                        |> Observable.timeoutSpanOption timeout
                        |> Async.AwaitObservable

            return Option.isSome ack
        }

        member x.SendPing seqNr memb = async {
            do! PingMessage seqNr |> Message.encode |> x.Transport.Send (memb.Address)
            let! acked = x.WaitForAck seqNr memb x.PingTimeout

            return acked
        }

        member x.SendPingRequest seqNr memb = async {
            let send m = PingRequestMessage(seqNr, memb) |> Message.encode |> x.Transport.Send (m.Address)
            let members = x.MemberList.Members()
                            |> List.shuffle
                            |> List.choose (fun (m, _) -> if m <> memb then Some m else None)
            
            do! members
                |> List.take (Math.Min(x.PingRequestGroupSize, List.length members))
                |> List.map send
                |> List.toSeq
                |> Async.Parallel
                |> Async.Ignore
        }

        member x.Ping seqNr = async {
            let (memb, incarnation), state' = x.PickPingTarget()

            let! awaitForPeriodAck =
                [ x.WaitForAck seqNr memb x.PeriodTimeout
                  Async.Delay false x.PeriodTimeout ]
                |> Async.Parallel
                |> Async.StartChild

            let! pingAcked = x.SendPing seqNr memb
            if not pingAcked then
                do! x.SendPingRequest seqNr memb

            let! acked = awaitForPeriodAck
            if Array.head acked then
                printfn "Ping %A successfully" seqNr
                x.MemberList.Alive memb incarnation
            else
                printfn "Failed to ping %A" seqNr
                x.MemberList.Suspect memb incarnation

            return state'
        }

        member x.Ack (seqNr, from) endpoint = 
            AckMessage(seqNr, from) |> Message.encode |> x.Transport.Send endpoint

        member x.HandlePing seqNr endpoint = x.Ack (seqNr, x.Local) endpoint

        member x.HandlePingRequest (seqNr, memb) endpoint = async {
            let! acked = x.SendPing seqNr memb

            if acked then
                do! x.Ack (seqNr, memb) endpoint
        }

let private nextSeqNumber seqNr =
    if seqNr < UInt64.MaxValue then
        seqNr + 1UL
    else
        0UL

let rec private pingLoop seqNr (state : State) = async {
    let! state' = state.Ping seqNr
    return! pingLoop (nextSeqNumber seqNr) state'
}

let private handle addr msg (state : State) = Async.Start(async {
    match msg with
    | PingMessage seqNr ->
        return! state.HandlePing seqNr addr
    | PingRequestMessage pingRequest ->
        return! state.HandlePingRequest pingRequest addr
    | _ -> ()
})

type Config =
    { Port: int
      Local : Member
      MemberList : MemberList
      PeriodTimeout : TimeSpan
      PingTimeout : TimeSpan
      PingRequestGroupSize : int }

let init config =
    let transport = Transport.create config.Port

    let state =
        { Transport = transport
          Local = config.Local
          MemberList = config.MemberList
          PingTargets = config.MemberList.Members() |> List.shuffle
          PeriodTimeout = config.PeriodTimeout
          PingTimeout = config.PingTimeout
          PingRequestGroupSize = config.PingRequestGroupSize }

    Async.Start(pingLoop 0UL state)

    printfn "Failure detection system is running"    
    transport.Received
    |> Observable.map (fun (addr, bytes) -> addr, Message.decode bytes)
    |> Observable.filter (fun (addr, msg) -> Option.isSome msg)
    |> Observable.subscribe (fun (addr, msg) -> handle addr (Option.get msg) state)
