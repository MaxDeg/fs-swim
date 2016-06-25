module SwimProtocol.Swim

open System
open Membership
open Transport
open FailureDetection

type Host = string * int

type Config = 
    { Port: int
      PeriodTimeout : TimeSpan
      PingTimeout: TimeSpan
      PingRequestGroupSize : int
      SuspectTimeout : TimeSpan }

let defaultConfig =
    { Port = 1337
      PeriodTimeout = TimeSpan.FromSeconds 2.0
      PingTimeout = TimeSpan.FromMilliseconds 200.0
      PingRequestGroupSize = 3
      SuspectTimeout = TimeSpan.FromSeconds 10.0 }

let private decodeMessage (addr, bytes) =
    Message.decode bytes
    |> Option.map (fun msg -> addr, msg)

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
        hosts
        |> List.map (fun (h, p) -> MemberList.makeMember h p)
        |> MemberList.createWith disseminator suspectTimeout

    udp.Received
    |> Observable.choose decodeMessage
    // dispach event to disseminator
    |> FailureDetection.run { Local = local
                              MemberList = memberList
                              PeriodTimeout = periodTimeout
                              PingTimeout = pingTimeout
                              PingRequestGroupSize = pingReqGrpSize }
    |> Observable.map (fun (addr, msg) -> addr, Message.encode msg)
    // Add event from disseminator
    |> Observable.subscribe udp.Send
