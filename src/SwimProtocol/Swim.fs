module SwimProtocol.Swim

open System
open Membership
open Transport
open FailureDetection
open FSharp.Control
open FSharp.Control.Reactive
open FSharpx.Control.Observable

type Host = string * int

type Config = 
    { Port: int
      PeriodTimeout : TimeSpan
      PingTimeout: TimeSpan
      PingRequestGroupSize : int
      SuspectTimeout : TimeSpan }

let defaultConfig =
    { Port = 1337
      PeriodTimeout = TimeSpan.FromSeconds 2.
      PingTimeout = TimeSpan.FromMilliseconds 300.
      PingRequestGroupSize = 3
      SuspectTimeout = TimeSpan.FromSeconds 2. }

let private decodeMessage (addr, bytes) =
    Message.decode bytes
    |> Option.map (fun (msg, events) -> (addr, msg), events)

let init config hosts = 
    let { Port = port
          PeriodTimeout = periodTimeout
          PingTimeout = pingTimeout
          PingRequestGroupSize = pingReqGrpSize
          SuspectTimeout = suspectTimeout } = config

    let local = MemberList.makeLocal port
    let disseminator = Dissemination.create()
    let udp = Udp.connect port
        
    let memberList =
        hosts |> List.map (fun (h, p) -> MemberList.makeMember h p)
              |> MemberList.createWith disseminator suspectTimeout

    let encodeMessage msg =
        Message.encode msg (memberList.Length |> disseminator.Pull)

    let messageReceived = udp.Received |> Observable.choose decodeMessage
    
    let diss =
        messageReceived |> Observable.map snd

    let failureDetection =
        messageReceived |> Observable.map fst
                        |> FailureDetection.run { Local = local
                                                  MemberList = memberList
                                                  PeriodTimeout = periodTimeout
                                                  PingTimeout = pingTimeout
                                                  PingRequestGroupSize = pingReqGrpSize }
                        |> Observable.map (fun (addr, msg) -> addr, encodeMessage msg)
                        |> Observable.subscribe udp.Send

    { new IDisposable with
        member __.Dispose() =
            failureDetection.Dispose() }    
