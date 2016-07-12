namespace SwimProtocol
    
type private FailureDetectionRequest =
    | ProtocolPeriod of SeqNumber
    | PingTimeout of SeqNumber * Node
    | Message of Node * SwimMessage
    
type FailureDetection = private FailureDetection of Agent<FailureDetectionRequest>

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module FailureDetection =
    [<AutoOpen>]
    module private State =
        open System
        open FSharpx.State

        type private PingMessage = Node * IncarnationNumber * SeqNumber

        type State = { Local : Node
                       MemberList : MemberList
                       RoundRobinNodes : (Node * IncarnationNumber) list
                       Sender : Node -> SwimMessage -> unit
                       PingTimeout : TimeSpan
                       PingRequestGroupSize : int
                       Ping : PingMessage option
                       PingRequests : Map<Node * SeqNumber, Node> }

        let rec private roundRobinNode state =
            match state.RoundRobinNodes with
            | head::tail ->
                Some head, { state with RoundRobinNodes = tail }
            | [] when MemberList.length state.MemberList = 0 ->
                None, state
            | [] -> 
                roundRobinNode { state with RoundRobinNodes = MemberList.members state.MemberList
                                                              |> List.shuffle }
    
        let forwardPing source seqNr node state =
            sprintf "Ping request from %O (%A) - forwarding to %O" source seqNr node |> trace
            Ping seqNr |> state.Sender node
            { state with PingRequests = Map.add (node, seqNr) source state.PingRequests }

        let ack source seqNr state =
            sprintf "Ping from %O (%A) - acking" source seqNr |> trace
            MemberList.update source (IncarnationNumber.make() |> Alive) state.MemberList
            Ack(seqNr, state.Local) |> state.Sender source
            state
        
        let handleAck seqNr node state =
            match state.Ping, Map.tryFind (node, seqNr) state.PingRequests with
            | Some(pingNode, pingInc, pingSeqNr), None when node = pingNode && seqNr = pingSeqNr ->
                sprintf "Ack received from %O (%A)" node seqNr |> trace
                MemberList.update node (Alive pingInc) state.MemberList
                { state with Ping = None }

            | None, Some target ->
                Ack(seqNr, node) |> state.Sender target
                sprintf "PingRequest Acked forward to %O" target |> trace
                { state with PingRequests = Map.remove (node, seqNr) state.PingRequests }

            | _ -> 
                state
            
        let runProtocolPeriod (agent : Agent<FailureDetectionRequest>) seqNr =
            let ping node inc state =
                Ping seqNr |> state.Sender node
                state.Ping, { state with Ping = Some(node, inc, seqNr) }
                
            let schedulePingTimeout node state =
                Agent.postAfter agent (PingTimeout(seqNr, node)) state.PingTimeout
                (), state

            let suspect node inc state =
                MemberList.update node (Suspect inc) state.MemberList
                (), state

            state {
                let! selectedNode = roundRobinNode
                match selectedNode with
                | None -> ()
                | Some(node, inc) ->
                    sprintf "Run protocol %A ping %O" seqNr node |> trace
                    let! unackedPing = ping node inc
                    do! schedulePingTimeout node

                    match unackedPing with
                    | Some(unackedNode, inc, _) ->
                        sprintf "Suspect unacked ping %O" unackedNode |> trace
                        do! suspect unackedNode inc
                    | None -> ()
            } |> exec
        
        let pingRequest seqNr node state =
            match state.Ping with
            | Some(_, _, pingSeqNr) when seqNr = pingSeqNr ->
                let nodes = MemberList.members state.MemberList
                            |> List.filter (fun (n, _) -> n <> node)
                            |> List.map fst
                            |> List.shuffle

                nodes |> List.take (Math.Min(List.length nodes, state.PingRequestGroupSize))
                      |> List.iter (fun n -> PingRequest(seqNr, node) |> state.Sender n)
                state

            | _ -> state

    let make local timeout pingRequestGrouSize memberList sender =
        let handler agent state = function
            | ProtocolPeriod seqNr ->
                (runProtocolPeriod agent seqNr) state

            | PingTimeout(seqNr, node) -> 
                pingRequest seqNr node state

            | Message(source, Ping seqNr) -> 
                ack source seqNr state

            | Message(source, PingRequest(seqNr, node)) -> 
                forwardPing source seqNr node state

            | Message(_, Ack(seqNr, node)) -> 
                handleAck seqNr node state

            | Message _ -> state

        handler |> Agent.spawn { Local = local
                                 MemberList = memberList
                                 RoundRobinNodes = []
                                 Sender = sender
                                 PingTimeout = timeout
                                 PingRequestGroupSize = pingRequestGrouSize
                                 Ping = None
                                 PingRequests = Map.empty }
                |> FailureDetection

    let handle node msg (FailureDetection agent) =
        Message(node, msg) |> Agent.post agent

    let protocolPeriod seqNr (FailureDetection agent) =
        ProtocolPeriod seqNr |> Agent.post agent
