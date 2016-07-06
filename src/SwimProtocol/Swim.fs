namespace SwimProtocol

open System
open System.Net
open System.Net.Sockets
open FSharpx.State
open FSharpx.Option

type Config =
    { Port : uint16
      PeriodTimeout : TimeSpan
      PingTimeout : TimeSpan
      PingRequestGroupSize : int }
    
type private PingMessage = Node * IncarnationNumber * SeqNumber

type Swim =
    private { Config : Config
              Udp : Udp
              Local : Node
              MemberList : MemberList
              RoundRobinNodes : (Node * IncarnationNumber) list
              Disseminator : Disseminator
              Ping : PingMessage option
              PingRequests : Map<Node * SeqNumber, Node> }

module private FailureDetection =
    let private sender state =
        let events, disseminator = Disseminator.take (MemberList.length state.MemberList) Udp.MaxSize state.Disseminator
        
        (fun node msg ->
            let encodedMsg = Message.encodeMessage msg
            Udp.send node (Message.encode encodedMsg events) state.Udp),
        { state with Disseminator = disseminator }

    let ping node inc seqNr state =
        let sender, state = sender state

        printfn "%O ping %O" state.Local node
        Ping seqNr |> sender node
        state.Ping, { state with Ping = Some(node, inc, seqNr) }

    let pingRequest nodes seqNr node state =
        let sender, state = sender state

        nodes |> List.iter (fun n -> PingRequest(seqNr, node) |> sender n)
        (), state

    let forwardPing source seqNr node state =
        let sender, state = sender state

        Ping seqNr |> sender source
        (), { state with PingRequests = Map.add (node, seqNr) source state.PingRequests }

    let ack source seqNr state =
        let sender, state = sender state
        
        printfn "%O ping %O" state.Local source
        Ack(seqNr, state.Local) |> sender source
        (), state
    
    let handleAck seqNr node state =
        match state.Ping, Map.tryFind (node, seqNr) state.PingRequests with
        | Some(pingNode, pingInc, pingSeqNr), None when node = pingNode && seqNr = pingSeqNr ->
            Some pingInc, { state with Ping = None }

        | None, Some target ->
            let sender, state = sender state

            Ack(seqNr, node) |> sender target
            None, { state with PingRequests = Map.remove (node, seqNr) state.PingRequests }

        | _ -> 
            None, state

    let leave state =
        let sender, state = sender state
        let _, inc = MemberList.local state.MemberList

        MemberList.members state.MemberList
        |> List.iter (fun (n, _) -> SwimMessage.Leave inc |> sender n)

        (), state

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Swim =
    type private Message =
        | IncomingMessage of Node * SwimMessage * SwimEvent list
        | ProtocolPeriod of SeqNumber
        | PingTimeout of SeqNumber * Node
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
    
    let rec private roundRobinNode state =
        match state.RoundRobinNodes with
        | [] -> 
            roundRobinNode { state with RoundRobinNodes = MemberList.members state.MemberList
                                                          |> List.shuffle }
        | head::tail ->
            head, { state with RoundRobinNodes = tail }
        
    let private getRandomNodes node state =
        let nodes = MemberList.members state.MemberList
                    |> List.filter (fun (n, _) -> n = node)
                    |> List.map fst
                    |> List.shuffle

        nodes |> List.take (Math.Min(List.length nodes, state.Config.PingRequestGroupSize)),
        state
    
    let private schedulePingTimeout (agent : Agent<Message>) seqNr node state =
        agent.PostAfter (PingTimeout(seqNr, node)) state.Config.PingTimeout
        (), state

    let private schedulePeriodTimeout (agent : Agent<Message>) seqNr state =
        agent.PostAfter (Sequence.incr seqNr |> ProtocolPeriod) state.Config.PeriodTimeout
        (), state

    let private updateMemberList node status state =
        let nodeStatus, memberList = MemberList.update node status state.MemberList
        match nodeStatus with
        | Some(n, s) ->
            (), { state with MemberList = memberList
                             Disseminator = Disseminator.membership n s state.Disseminator }
        | None ->
            (), { state with MemberList = memberList }

    let private pushEvents events state =
        let disseminator =
            List.fold (fun disseminator evt ->
                match evt with
                | Membership(n, s) -> Disseminator.membership n s disseminator
                | User e -> Disseminator.user e disseminator) state.Disseminator events

        (), { state with Disseminator = disseminator }

    let private runProtocolPeriod (agent : Agent<Message>) seqNr = state {
        let! node, inc = roundRobinNode
        let! unackedPing = FailureDetection.ping node inc seqNr
    
        do! schedulePingTimeout agent seqNr node
        do! schedulePeriodTimeout agent seqNr

        match unackedPing with
        | Some(unackedNode, inc, _) ->
            do! updateMemberList unackedNode (Suspect inc)
        | None -> ()
    }

    let private handle agent msg = state {
        match msg with
        | ProtocolPeriod seqNr ->
            return! runProtocolPeriod agent seqNr

        | PingTimeout(seqNr, node) ->
            let! nodes = getRandomNodes node
            return! FailureDetection.pingRequest nodes seqNr node

        | IncomingMessage(source, Ping seqNr, events) ->
            do! pushEvents events
            return! FailureDetection.ack source seqNr

        | IncomingMessage(source, PingRequest(seqNr, node), events) ->
            do! pushEvents events
            return! FailureDetection.forwardPing source seqNr node

        | IncomingMessage(_, Ack(seqNr, node), events) ->
            do! pushEvents events
            let! acked = FailureDetection.handleAck seqNr node

            match acked with
            | Some inc -> return! updateMemberList node (Alive inc)
            | None -> ()

        | IncomingMessage(source, SwimMessage.Leave inc, events) ->
            do! pushEvents events
            return! updateMemberList source (Dead inc)

        | Leave -> return! FailureDetection.leave
    }

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
        let memberList = MemberList.make local members
        let disseminator = Disseminator.make()
    
        let state = { Config = config
                      Udp = udp
                      Local = local
                      MemberList = memberList
                      RoundRobinNodes = MemberList.members memberList |> List.shuffle
                      Disseminator = disseminator
                      Ping = None
                      PingRequests = Map.empty }
    
        let agent = Agent<Message>.Start(fun agent ->
            let rec loop state = async {
                let! msg = agent.Receive()
                return! exec (handle agent msg) state
                        |> loop
            }
        
            loop state)
    
        Udp.received udp
        |> Event.add (fun (node, msg) ->
            maybe { let! message, events = Message.decode msg
                    IncomingMessage(node, message, events) |> agent.Post }
            |> ignore)

        agent.Post(Sequence.make() |> ProtocolPeriod)

        state

    let connect node swim =
        updateMemberList node (IncarnationNumber.make() |> Alive) swim |> snd
