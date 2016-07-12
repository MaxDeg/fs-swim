namespace SwimProtocol

open System
open System.Net
open System.Net.Sockets
open FSharpx.Option

type Config =
    { Port : uint16
      PeriodTimeout : TimeSpan
      PingTimeout : TimeSpan
      PingRequestGroupSize : int }

type Swim =
    private { Config : Config
              Udp : Udp
              MemberList : MemberList
              Dissemination : Dissemination
              FailureDetection : FailureDetection }

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Swim =    
    let makeNode (host : string) port =
        // Issue with ip 127.0.0.1 with UdpClient
        // localhost only return 127.0.0.1 host entry
        // 127.0.0.1 return the correct ip
        let ipAddress =
            Dns.GetHostEntry(if host = "localhost" then "127.0.0.1" else host).AddressList
            |> Array.filter (fun a -> a.AddressFamily = AddressFamily.InterNetwork)
            |> Array.head
        
        { IPAddress = ipAddress.GetAddressBytes() |> toInt64
          Port = port }

    let private makeLocalNode port =
        let hostName = Dns.GetHostName()
        makeNode hostName port

    let private pushEvents events state =
        List.iter (fun evt ->
            match evt with
            | Membership(n, s) -> MemberList.update n s state.MemberList
            | User e -> Dissemination.user e state.Dissemination) events

    let private sender dissemination memberList udp node msg =
        let events = Dissemination.take (MemberList.length memberList) Udp.MaxSize dissemination
        let encodedMsg = Message.encodeMessage msg
        Udp.send node (Message.encode encodedMsg events) udp

    let defaultConfig =
        { Port = 1337us
          PeriodTimeout = TimeSpan.FromSeconds(2.)
          PingTimeout = TimeSpan.FromMilliseconds(500.)
          PingRequestGroupSize = 3 }

    // make : unit -> IDisposable
    let start config nodes =
        let local = makeLocalNode config.Port
        let udp = Udp.connect config.Port
        let members = nodes |> List.map (fun (h, p) -> makeNode h p)
        let dissemination = Dissemination.make()
        let memberList = MemberList.make local members dissemination config.PeriodTimeout
        let sender = sender dissemination memberList udp
        let failureDetection = FailureDetection.make local config.PingTimeout config.PingRequestGroupSize memberList sender
        let swim = { Config = config
                     Udp = udp
                     MemberList = memberList
                     Dissemination = dissemination
                     FailureDetection = failureDetection }

        Udp.received udp |> Event.add (fun (node, msg) -> ignore <| maybe {
            let! message, events = Message.decode msg
            pushEvents events swim

            match message with
            | SwimMessage.Leave inc -> MemberList.update node (Dead inc) swim.MemberList
            | swimMessage -> FailureDetection.handle node swimMessage swim.FailureDetection
        })
                
        let rec protocolPeriod seqNr = async {
            FailureDetection.protocolPeriod seqNr swim.FailureDetection
            do! Async.Sleep(int swim.Config.PeriodTimeout.TotalMilliseconds)
            return! Sequence.incr seqNr |> protocolPeriod
        }
        Async.Start(Sequence.make() |> protocolPeriod)

        sprintf "Swim protocol running on port %i" config.Port |> info
        swim

    let members swim =
        MemberList.members swim.MemberList

    let events swim =
        Dissemination.show swim.Dissemination
    
    let stop swim =
        MemberList.members swim.MemberList
        |> List.iter (fun (m, i) -> sender swim.Dissemination swim.MemberList swim.Udp m (Leave i))

