namespace SwimProtocol

open System

type MemberList =
    private { Local : LocalNode
              Members : Map<Node, NodeStatus> }

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module MemberList =
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

        // disseminate information
        Some(memb, status),
        { memberList with Members = members' }

    // stillAlive : MemberList -> IncarnationNumber -> MemberList
    let private stillAlive incarnation memberList =
        let { Local = local, currentIncarnation } = memberList

        let incarnation' =  if currentIncarnation > incarnation then currentIncarnation
                            else IncarnationNumber.incr currentIncarnation

        // disseminate information
        Some(local, Alive incarnation'),
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

        | Member _ -> None, memberList
        | Local _ -> None, memberList

    // suspect : MemberList -> Node -> IncarnationNumber -> MemberList
    let private suspect memb incarnation memberList =
        match tryFind memb memberList with
        | Local i -> stillAlive i memberList

        | Member(Alive i) when incarnation >= i ->
            updateMembers memb (Suspect incarnation) memberList

        | Member(Suspect i) when incarnation > i ->
            updateMembers memb (Suspect incarnation) memberList

        | Member _ -> None, memberList
        | NotFound -> None, memberList

    // dead : MemberList -> Node -> IncarnationNumber -> MemberList
    let private dead memb incarnation memberList =
        match tryFind memb memberList with
        | Local i -> stillAlive i memberList

        | Member(Alive i)
        | Member(Suspect i) when incarnation >= i ->
            updateMembers memb (Dead i) memberList

        | Member(Dead i) when incarnation > i ->
            updateMembers memb (Dead i) memberList

        | Member _ -> None, memberList
        | NotFound -> None, memberList

    // make : Node -> TimeSpan -> Node list -> MemberList
    let make local members =
        { Local = local, IncarnationNumber.make()
          Members = members |> List.map (fun m -> m, Alive(IncarnationNumber.make())) |> Map.ofList }
    
    // update : MemberList -> Node -> Status -> MemberList
    let update node status memberList =
        match status with
        | Alive i -> alive node i memberList
        | Suspect i -> suspect node i memberList
        | Dead i -> dead node i memberList

    let members memberList =
        memberList.Members
        |> Map.toList
        |> List.map (function n, Alive i | n, Suspect i | n, Dead i -> n, i)

    let length memberList =
        memberList.Members.Count

    let tryGet node memberList =
        match tryFind node memberList with
        | Local i
        | Member(Alive i)
        | Member(Suspect i) -> Some i
        | _ -> None

    let local memberList = memberList.Local
        