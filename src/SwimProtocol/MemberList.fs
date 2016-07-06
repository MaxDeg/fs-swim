module SwimProtocol.MemberList

open System
open FSharpx.State

type private State =
     { Local : LocalNode
       Members : Map<Node, NodeStatus>
       Disseminator : Disseminator }

type private FindResult =
    | Local of IncarnationNumber
    | Member of NodeStatus
    | NotFound
    
// tryFind : MemberList -> Node -> FindResult
let private tryFind memb memberList =
    let { Local = local, currentIncarnation
          Members = members } = memberList

    if memb = local then
        Local(currentIncarnation)
    else
        match Map.tryFind memb members with
        | Some s -> Member(s)
        | None -> NotFound

// updateMembers : MemberList -> Node -> NodeStatus -> MemberList
let private updateMembers memb status memberList =
    let { Members = members } = memberList
    let members' = Map.add memb status members

    printfn "%O MemberList Status: %O is %A" memberList.Local memb status
    // disseminate information
    // Some(memb, status)

    { memberList with Members = members' }

// stillAlive : MemberList -> IncarnationNumber -> MemberList
let private stillAlive incarnation memberList =
    let { Local = local, currentIncarnation } = memberList

    let incarnation' =  if currentIncarnation > incarnation then currentIncarnation
                        else IncarnationNumber.incr currentIncarnation

    // disseminate information
    //Some(local, Alive incarnation')

    { memberList with Local = local, incarnation' }

// alive : MemberList -> Node -> IncarnationNumber -> MemberList
let private alive memb incarnation memberList =
    match tryFind memb memberList with
    | NotFound ->
        let status = IncarnationNumber.make() |> Alive
        updateMembers memb status memberList

    | Member(Alive i)
    | Member(Suspect i) when i < incarnation ->
        updateMembers memb (Alive incarnation) memberList

    | Member _ -> memberList
    | Local _ -> memberList

// suspect : MemberList -> Node -> IncarnationNumber -> MemberList
let private suspect memb incarnation memberList =
    match tryFind memb memberList with
    | Local i -> stillAlive i memberList

    | Member(Alive i) when incarnation >= i ->
        updateMembers memb (Suspect incarnation) memberList

    | Member(Suspect i) when incarnation > i ->
        updateMembers memb (Suspect incarnation) memberList

    | Member _ -> memberList
    | NotFound -> memberList

// dead : MemberList -> Node -> IncarnationNumber -> MemberList
let private dead memb incarnation memberList =
    match tryFind memb memberList with
    | Local i -> stillAlive i memberList

    | Member(Alive i)
    | Member(Suspect i) when incarnation >= i ->
        updateMembers memb (Dead i) memberList

    | Member(Dead i) when incarnation > i ->
        updateMembers memb (Dead i) memberList

    | Member _ -> memberList
    | NotFound -> memberList


type private Request =
    | Status of Node * NodeStatus
    | Members of AsyncReplyChannel<(Node * IncarnationNumber) list>
    | Length of AsyncReplyChannel<int>
    | Local of AsyncReplyChannel<LocalNode>

let private handle req state =
    match req with
    | Status(node, Alive incarnation) -> alive node incarnation state
    | Status(node, Suspect incarnation) -> suspect node incarnation state
    | Status(node, Dead incarnation) -> dead node incarnation state
    
    | Local reply -> 
        let node = state.Local
        reply.Reply(node)
        state

    | Members reply ->
        reply.Reply(state.Members 
                    |> Map.toList
                    |> List.map (function n, Alive i | n, Suspect i | n, Dead i -> n, i))
        state

    | Length reply ->
        reply.Reply(state.Members.Count)
        state

// make : Node -> TimeSpan -> Node list -> MemberList
let make local members disseminator =
    let state =
        { Local = local, IncarnationNumber.make()
          Members = members |> List.map (fun m -> m, Alive(IncarnationNumber.make())) |> Map.ofList
          Disseminator = disseminator }
    
    Agent<Request>.Start(fun box ->
        let rec loop state = async {
            let! msg = box.Receive()
            return! (handle msg state) |> loop
        }
        
        loop state)


// update : MemberList -> Node -> Status -> MemberList
let update node status memberList =
    Status(node, status) |> memberList.Post

let members memberList =
    memberList.Members |> Map.toList
                       |> List.map (function n, Alive i | n, Suspect i | n, Dead i -> n, i),
    memberList

let length memberList =
    memberList.Members.Count, memberList

let tryGet node memberList =
    match tryFind node memberList with
    | Local i
    | Member(Alive i)
    | Member(Suspect i) -> Some i
    | _ -> None

let local memberList = memberList.Local, memberList
