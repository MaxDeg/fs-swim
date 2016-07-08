namespace SwimProtocol

open System
open System.Net
open System.Net.Sockets
open FSharpx.State
open FSharpx.Option

open MemberList
open Dissemination
open FailureDetection

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
    type private Message =
        | IncomingMessage of Node * SwimMessage * SwimEvent list
        | ProtocolPeriod of SeqNumber
        | Leave
        
    let makeNode host port =
        let ipAddress =
            Dns.GetHostAddresses(host)
            |> Array.filter (fun a -> a.AddressFamily = AddressFamily.InterNetwork)
            |> Array.head
        
        { IPAddress = ipAddress.GetAddressBytes() |> toInt64
          Port = port }

    let private makeLocalNode port =
        let hostName = Dns.GetHostName()
        makeNode hostName port

    let private schedulePeriodTimeout (agent : Agent<Message>) seqNr state =
        agent.PostAfter (Sequence.incr seqNr |> ProtocolPeriod) state.Config.PeriodTimeout
        (), state

    let private pushEvents events state =
        List.iter (fun evt ->
            match evt with
            | Membership(n, s) -> Dissemination.membership n s state.Dissemination
            | User e -> Dissemination.user e state.Dissemination) events

    let private sender dissemination memberList udp =
        let events = Dissemination.take (MemberList.length memberList) Udp.MaxSize dissemination
        
        (fun node msg ->
            let encodedMsg = Message.encodeMessage msg
            Udp.send node (Message.encode encodedMsg events) udp)

    let private handle agent msg state =
        match msg with
        | ProtocolPeriod seqNr ->
            Agent.postAfter agent (Sequence.incr seqNr |> ProtocolPeriod) state.Config.PeriodTimeout
            FailureDetection.protocolPeriod seqNr state.FailureDetection
        
        | IncomingMessage(source, SwimMessage.Leave inc, events) ->
            pushEvents events state
            MemberList.update source (Dead inc) state.MemberList

        | IncomingMessage(source, swimMsg, events) ->
            pushEvents events state
            FailureDetection.handle source swimMsg state.FailureDetection 

        | Leave ->
            let sender = sender state.Dissemination state.MemberList state.Udp
            MemberList.members state.MemberList
            |> List.iter (fun (m, i) -> sender m (SwimMessage.Leave i))

        state

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

        let handler agent state msg =
            match msg with
            | ProtocolPeriod seqNr ->
                Agent.postAfter agent (Sequence.incr seqNr |> ProtocolPeriod) state.Config.PeriodTimeout
                FailureDetection.protocolPeriod seqNr state.FailureDetection
        
            | IncomingMessage(source, SwimMessage.Leave inc, events) ->
                pushEvents events state
                MemberList.update source (Dead inc) state.MemberList

            | IncomingMessage(source, swimMsg, events) ->
                pushEvents events state
                FailureDetection.handle source swimMsg state.FailureDetection

            | Leave ->
                MemberList.members state.MemberList
                |> List.iter (fun (m, i) -> sender m (SwimMessage.Leave i))

            state

        let agent = Agent.spawn { Config = config
                                  Udp = udp
                                  MemberList = memberList
                                  Dissemination = dissemination
                                  FailureDetection = failureDetection }
                                handler
    
        let decodeMessage (node, msg) = ignore <| maybe {
            let! message, events = Message.decode msg
            IncomingMessage(node, message, events) |> agent.Post
        }

        Udp.received udp |> Event.add decodeMessage
        agent.Post(Sequence.make() |> ProtocolPeriod)

        state

//    let connect node swim =
//        updateMemberList node (IncarnationNumber.make() |> Alive) swim |> snd

