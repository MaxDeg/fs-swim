module SwimProtocol.FailureDetection

open FSharpx.Option
open FSharp.Control
open System
open System.Net
open Transport
open Message


type private State = 
    { SendMessage: IPEndPoint -> Message -> Async<unit>
      AckAwaiter: (Ack -> bool) -> TimeSpan -> Async<Ack option>
      Local : Member
      MemberList : MemberList.T
      PingTargets : (Member * IncarnationNumber) list
      PeriodTimeout : TimeSpan
      PingTimeout : TimeSpan
      PingRequestGroupSize : int }

type AckMessage =
| Push of Ack
| Await of AsyncReplyChannel<Ack> * (Ack -> bool)
     
let private ackDispatcher () =
    let agent = 
        Agent<AckMessage>.Start(fun box ->
            let rec loop channels = async {
                let! msg = box.Receive()
                match msg with
                | Push ack -> 
                    let toSend, toKeep = List.partition (fun (_, filter) -> filter ack) channels
                    toSend |> List.iter (fun (chan : AsyncReplyChannel<Ack>, _) -> chan.Reply(ack))
                    
                    return! loop toKeep
                | Await(chan, filter) -> 
                    return! (chan, filter) :: channels |> loop    
            }
            
            loop List.empty)
    
    let push ack = Push ack |> agent.Post
    let awaiter filter (timeout: TimeSpan) = 
        agent.PostAndTryAsyncReply((fun chan -> Await(chan, filter)), int timeout.TotalMilliseconds)
    
    push, awaiter 

let rec private pickPingTarget ({ MemberList = memberList; PingTargets = pingTargets } as state) = 
    match pingTargets with
    | [] -> pickPingTarget { state with PingTargets = MemberList.members memberList |> List.shuffle }
    | head::tail -> head, { state with PingTargets = tail }

let private waitForAck seqNr memb (timeout: TimeSpan) { AckAwaiter = awaiter } = async {
    let! ack = awaiter (fun (ackSeqNr, ackMemb) -> seqNr = ackSeqNr && memb = ackMemb) timeout
    return Option.isSome ack
}

let private sendPing seqNr memb state = async {
    let { PingTimeout = pingTimeout;
          SendMessage = sendMsg } = state
    
    do! Ping seqNr |> sendMsg (memb.Address)
    let! acked = waitForAck seqNr memb pingTimeout state
    
    return acked
}

let private sendPingRequest seqNr memb state = async {
    let { MemberList = memberList
          PingRequestGroupSize = pingRequestGroupSize
          SendMessage = sendMsg } = state
    
    let send m = PingRequest(seqNr, memb) |> sendMsg (m.Address)
    
    let members = MemberList.members memberList 
                  |> List.choose (fun (m, _) -> if m <> memb then Some m else None)
    
    do! members        
        |> List.take (Math.Min(pingRequestGroupSize, List.length members))
        |> List.map send
        |> List.toSeq
        |> Async.Parallel
        |> Async.Ignore
}

let private ack (seqNr, from) endpoint { SendMessage = sendMsg } = 
    Ack(seqNr, from) |> sendMsg endpoint

let private handlePingRequest (seqNr, memb) endpoint state = async {
    let! acked = sendPing seqNr memb state

    if acked then
        do! ack (seqNr, memb) endpoint state
}

let private handlePing seqNr endpoint ({ Local = local } as state) =
    ack (seqNr, local) endpoint state


let private ping seqNr state = 
    async {
        let { MemberList = memberList
              PeriodTimeout = periodTimeout } = state
        let (memb, incarnation), state' = pickPingTarget state
        printfn "Ping %A" seqNr
        let! awaitForPeriodAck = 
            [ waitForAck seqNr memb periodTimeout state
              async { do! int periodTimeout.TotalMilliseconds |> Async.Sleep
                      return false } ]
            |> Async.Parallel
            |> Async.StartChild

        let! pingAcked = sendPing seqNr memb state

        if not pingAcked then
            do! sendPingRequest seqNr memb state

        let! acked = awaitForPeriodAck

        if Array.head acked then
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

let private handle addr msg ackPusher state = async {
    printfn "Handle msg %A" msg
    match msg with
    | Ping seqNr -> 
        return! handlePing seqNr addr state
    | PingRequest pingRequest -> 
        return! handlePingRequest pingRequest addr state
    | Ack ack -> ackPusher ack 
}

type Config = 
    { Port: int
      Local : Member
      MemberList : MemberList.T
      PeriodTimeout : TimeSpan
      PingTimeout : TimeSpan
      PingRequestGroupSize : int }

let init config =
    let sender, receiver = Transport.create config.Port
    let push, awaiter = ackDispatcher()
    
    let state = 
        { SendMessage = (fun addr msg -> Message.encode msg |> sender addr)
          AckAwaiter = awaiter
          Local = config.Local
          MemberList = config.MemberList
          PingTargets = MemberList.members config.MemberList |> List.shuffle
          PeriodTimeout = config.PeriodTimeout
          PingTimeout = config.PingTimeout
          PingRequestGroupSize = config.PingRequestGroupSize }
    
    Async.Start(
        receiver
        |> AsyncSeq.choose (fun (addr, bytes) -> maybe { let! msg = Message.decode bytes 
                                                         return addr, msg })
        |> AsyncSeq.iterAsync (fun (addr, msg) -> handle addr msg push state))
    
    Async.Start(pingLoop 255UL state)

    printfn "Failure detection system is running"
