module SwimProtocol.MemberList

open Dissemination

type private Request =
    | Status of Node * NodeStatus
    | Members of AsyncReplyChannel<(Node * IncarnationNumber) list>
    | Length of AsyncReplyChannel<int>
    | GetLocal of AsyncReplyChannel<LocalNode>
    | Remove of Node

type MemberList = private MemberList of Agent<Request>

[<RequireQualifiedAccess; AutoOpen>]
module private State =
    open System

    type State =
            { Local : LocalNode
              PeriodTimeout : TimeSpan
              Members : Map<Node, NodeStatus>
              Dissemination : Dissemination }

    type FindResult =
        | Local of IncarnationNumber
        | Member of NodeStatus
        | NotFound

    let private suspectTimeout memb incarnation agent state = 
        (float state.Members.Count + 1. |> log |> round) * 5.0 * state.PeriodTimeout.TotalSeconds
        |> TimeSpan.FromSeconds
        |> Agent.postAfter agent (Status(memb, Dead incarnation))
    
    // tryFind : MemberList -> Node -> FindResult
    let tryFind memb state =
        let { Local = local, currentIncarnation
              Members = members } = state

        if memb = local then
            Local(currentIncarnation)
        else
            match Map.tryFind memb members with
            | Some s -> Member(s)
            | None -> NotFound

    // updateMembers : MemberList -> Node -> NodeStatus -> MemberList
    let updateMembers memb status state =
        let { Members = members } = state
        let members' =
            match status with
            | Dead i -> Map.remove memb members
            | _ -> Map.add memb status members

        printfn "%O MemberList Status: %O is %A" state.Local memb status
        // disseminate information
        Dissemination.membership memb status state.Dissemination

        { state with Members = members' }

    // stillAlive : MemberList -> IncarnationNumber -> MemberList
    let stillAlive incarnation state =
        let { Local = local, currentIncarnation } = state

        let incarnation' =  if currentIncarnation > incarnation then currentIncarnation
                            else IncarnationNumber.incr currentIncarnation

        // disseminate information
        Dissemination.membership local (Alive incarnation') state.Dissemination

        { state with Local = local, incarnation' }

    // alive : MemberList -> Node -> IncarnationNumber -> MemberList
    let alive memb incarnation state =
        match tryFind memb state with
        | NotFound ->
            let status = IncarnationNumber.make() |> Alive
            updateMembers memb status state

        | Member(Alive i)
        | Member(Suspect i) when i < incarnation ->
            updateMembers memb (Alive incarnation) state

        | Member _ -> state
        | Local _ -> state

    // suspect : MemberList -> Node -> IncarnationNumber -> MemberList
    let suspect memb incarnation agent state =
        match tryFind memb state with
        | Local i -> stillAlive i state

        | Member(Alive i) when incarnation >= i ->
            suspectTimeout memb incarnation agent state
            updateMembers memb (Suspect incarnation) state

        | Member(Suspect i) when incarnation > i ->
            suspectTimeout memb incarnation agent state
            updateMembers memb (Suspect incarnation) state

        | Member _ -> state
        | NotFound -> state

    // dead : MemberList -> Node -> IncarnationNumber -> MemberList
    let dead memb incarnation state =
        match tryFind memb state with
        | Local i -> stillAlive i state

        | Member(Alive i)
        | Member(Suspect i) when incarnation >= i ->
            updateMembers memb (Dead i) state

        | Member(Dead i) when incarnation > i ->
            updateMembers memb (Dead i) state

        | Member _ -> state
        | NotFound -> state

    let remove memb state =
        { state with Members = Map.remove memb state.Members }
        

(******* PUBLIC API *******)

// make : Node -> Node list -> Dissemination -> MemberList
let make local members dissemination periodTimeout =
    let handler agent state = function
    | Status(node, Alive incarnation) -> State.alive node incarnation state
    | Status(node, Suspect incarnation) -> State.suspect node incarnation agent state
    | Status(node, Dead incarnation) -> State.dead node incarnation state
    | Remove node -> State.remove node state
    
    | GetLocal reply ->
        reply.Reply(state.Local)
        state

    | Members reply ->
        reply.Reply(state.Members 
                    |> Map.toList
                    |> List.map (function n, Alive i | n, Suspect i | n, Dead i -> n, i))
        state

    | Length reply ->
        reply.Reply(state.Members.Count)
        state
        
    handler |> Agent.spawn { Local = local, IncarnationNumber.make()
                             PeriodTimeout = periodTimeout
                             Members = members |> List.map (fun m -> m, Alive(IncarnationNumber.make())) |> Map.ofList
                             Dissemination = dissemination }
            |> MemberList

// update : MemberList -> Node -> Status -> MemberList
let update node status (MemberList agent) = Status(node, status) |> Agent.post agent

let remove node (MemberList agent) = Remove node |> Agent.post agent

let members (MemberList agent) = Members |> Agent.postAndReply agent

let length (MemberList agent) =  Length |> Agent.postAndReply agent

let local (MemberList agent) = GetLocal |> Agent.postAndReply agent
