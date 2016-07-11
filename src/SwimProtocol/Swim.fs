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

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Swim =
    type private State =
         { Config : Config
           Udp : Udp
           MemberList : MemberList
           Dissemination : Dissemination
           FailureDetection : FailureDetection }

    type private Message =
        | IncomingMessage of Node * SwimMessage * SwimEvent list
        | ProtocolPeriod of SeqNumber
        | Members of AsyncReplyChannel<Member list>
        | Events of AsyncReplyChannel<SwimEvent list>
        | Leave

    type Swim = private Swim of Agent<Message>
    
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

        let handler agent state = function
            | ProtocolPeriod seqNr ->
                Agent.postAfter agent (Sequence.incr seqNr |> ProtocolPeriod) state.Config.PeriodTimeout
                FailureDetection.protocolPeriod seqNr state.FailureDetection
                state
        
            | IncomingMessage(source, SwimMessage.Leave inc, events) ->
                MemberList.update source (Dead inc) state.MemberList
                pushEvents events state
                state

            | IncomingMessage(source, swimMsg, events) ->
                FailureDetection.handle source swimMsg state.FailureDetection
                pushEvents events state
                state

            | Members replyChan ->
                MemberList.members state.MemberList |> replyChan.Reply
                state

            | Events replyChan ->
                Dissemination.show state.Dissemination |> replyChan.Reply
                state

            | Leave ->
                MemberList.members state.MemberList
                |> List.iter (fun (m, i) -> sender m (SwimMessage.Leave i))
                
                Agent.stop agent
                state

        let agent = Agent.spawn { Config = config
                                  Udp = udp
                                  MemberList = memberList
                                  Dissemination = dissemination
                                  FailureDetection = failureDetection }
                                handler
    
        let decodeMessage (node, msg) = ignore <| maybe {
            let! message, events = Message.decode msg
            IncomingMessage(node, message, events) |> Agent.post agent
        }

        Udp.received udp |> Event.add decodeMessage
        Sequence.make() |> ProtocolPeriod |> Agent.post agent

        sprintf "Swim protocol running on port %i" config.Port |> info
        Swim agent

    let members (Swim agent) =
        Members |> Agent.postAndReply agent

    let events (Swim agent) =
        Events |> Agent.postAndReply agent
    
    let stop (Swim agent) =
        Leave |> Agent.post agent

