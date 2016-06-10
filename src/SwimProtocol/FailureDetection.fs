module SwimProtocol.FailureDetection

open FSharp.Control.Reactive.Builders
open FSharpx.Control.Observable
open FSharpx.Option
open System
open System.Net

type T = 
    private
    | T of obj

type private State = 
    { Socket : Transport.Socket
      SocketListener : IObservable<IPEndPoint * DetectionMessage>
      Local : Member
      MemberList : MemberList.T
      PingTargets : (Member * IncarnationNumber) list
      PeriodTimeout : TimeSpan
      PingTimeout : TimeSpan
      PingRequestGroupSize : int }

let rec private pickPingTarget ({ MemberList = memberList; PingTargets = pingTargets } as state) = 
    match pingTargets with
    | [] -> pickPingTarget { state with PingTargets = MemberList.members memberList |> List.shuffle }
    | head::tail -> head, { state with PingTargets = tail }

let private waitForAck socketListener seqNr memb timeout =
    rxquery { 
        for (_, msg) in socketListener do
            match msg with
            | Ack(seq, m) when seq = seqNr && m = memb -> yield true
            | _ -> ()
    }
    |> Observable.await timeout
    |> Observable.map Option.isSome

let private sendPing seqNr memb state = async {
    let { PingTimeout = pingTimeout;
          Socket = socket;
          SocketListener = listener } = state
    
    do! Transport.encode (Ping seqNr) |> Transport.send socket (memb.Address)
    let! acked = waitForAck listener seqNr memb pingTimeout |> Async.AwaitObservable
    
    return acked
}

let private sendPingRequest seqNr memb state = async {
    let { MemberList = memberList;
          PingRequestGroupSize = pingRequestGroupSize;
          Socket = socket; } = state
    
    let send m = Transport.encode (PingRequest(seqNr, memb)) |> Transport.send socket (m.Address)
    
    let members = MemberList.members memberList |> List.choose (fun (m, _) -> if m <> memb then Some m else None)
    
    do! members        
        |> List.take (Math.Min(pingRequestGroupSize, List.length members - 1))
        |> List.map send
        |> List.toSeq
        |> Async.Parallel
        |> Async.Ignore
}

let private ack seqNr from endpoint { Socket = socket } = 
    Ack(seqNr, from) |> Transport.encode |> Transport.send socket endpoint

let private handlePingRequest seqNr target endpoint state = async {
    let! acked = sendPing seqNr target state

    if acked then
        do! ack seqNr target endpoint state
}

let private handlePing seqNr endpoint ({ Local = local } as state) =
    ack seqNr local endpoint state

let private ping seqNr state = 
    async { 
        let { MemberList = memberList; 
              PeriodTimeout = periodTimeout;
              SocketListener = listener } = state
        let (memb, incarnation), state' = pickPingTarget state
        
        let awaitForPeriodAck = waitForAck listener seqNr memb periodTimeout
        let! pingAcked = sendPing seqNr memb state
        let! acked = async {
            if pingAcked then return true
            else
                do! sendPingRequest seqNr memb state
                let! acked = awaitForPeriodAck |> Async.AwaitObservable
                return acked
        }

        if acked then
            MemberList.alive memberList memb incarnation
        else
            MemberList.suspect memberList memb incarnation

        return state'
    }

let private nextSeqNumber seqNr =
    if seqNr < UInt64.MaxValue then
        seqNr + 1UL
    else
        0UL

let rec private pingLoop seqNr state = async {
    let! state' = ping seqNr state
    return! pingLoop (nextSeqNumber seqNr) state'
}

type Config = 
    { Socket : Transport.Socket
      Local : Member
      MemberList : MemberList.T
      PeriodTimeout : TimeSpan
      PingTimeout : TimeSpan
      PingRequestGroupSize : int }

let init config =
    let listener = Transport.receive config.Socket 
                   |> Observable.choose (fun (addr, bytes) -> maybe { let! msg = Transport.decode bytes
                                                                      return addr, msg })
    
    let state = 
        { Socket = config.Socket
          SocketListener = listener
          Local = config.Local
          MemberList = config.MemberList
          PingTargets = MemberList.members config.MemberList |> List.shuffle
          PeriodTimeout = config.PeriodTimeout
          PingTimeout = config.PingTimeout
          PingRequestGroupSize = config.PingRequestGroupSize }

    Async.Start(pingLoop 0UL state)
    observe {
        let! addr, msg = listener
        match msg with
        | Ping seqNr -> Async.Start(handlePing seqNr addr state)
        | PingRequest(seqNr, memb) -> Async.Start(handlePingRequest seqNr memb addr state)
        | _ -> ()
    } |> ignore

    printfn "Failure detection system is running"
