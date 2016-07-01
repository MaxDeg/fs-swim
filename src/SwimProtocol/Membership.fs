module SwimProtocol.Membership

open FSharpx.Option
open System
open System.Net.NetworkInformation
open System.Net
open System.Net.Sockets
open FSharp.Control
open Dissemination

type private MemberStatus = 
    | Alive of IncarnationNumber
    | Suspected of IncarnationNumber
    | Dead of IncarnationNumber

type private Request = 
    | Revive of Node * IncarnationNumber
    | Suspect of Node * IncarnationNumber
    | Kill of Node * IncarnationNumber
    | Members of AsyncReplyChannel<(Node * IncarnationNumber) list>
    | Length of AsyncReplyChannel<int>

type private State = 
    { Local : Node
      Incarnation : IncarnationNumber
      Members : Map<Node, MemberStatus>
      PeriodTimeout: TimeSpan
      DeadMembers : Set<Node>
      Disseminator : EventDisseminator }

let private disseminate state evt =
    state.Disseminator.Push(MembershipEvent evt)

let private suspicionTimeout { PeriodTimeout = periodTimeout; Members = members } =
    (float members.Count + 1. |> log |> round) * 5.0 * periodTimeout.TotalSeconds |> TimeSpan.FromSeconds

let private updateMembers memb status state =
    match status with
    | Dead inc ->
        printfn "[%O] %O(%i) is Dead" state.Local memb inc
        MembershipEvent.Dead(memb, inc) |> disseminate state
        { state with Members = state.Members |> Map.remove memb
                     DeadMembers = state.DeadMembers |> Set.add memb }
    | Suspected inc ->
        printfn "[%O] %O(%i) is Suspect" state.Local memb inc
        MembershipEvent.Suspect(memb, inc) |> disseminate state
        { state with Members = state.Members |> Map.add memb status }
    | Alive inc ->
        printfn "[%O] %O(%i) is Alive" state.Local memb inc
        MembershipEvent.Alive(memb, inc) |> disseminate state
        { state with Members = state.Members |> Map.add memb status }

let private revive memb incarnation state = 
    if memb = state.Local then None
    else
        match state.Members |> Map.tryFind memb with
        | Some(Alive i) | Some(Suspected i) when incarnation > i ->
            updateMembers memb (Alive incarnation) state |> Some
        | Some _ -> None
        | None -> updateMembers memb (Alive incarnation) state |> Some

let private suspect memb incarnation (agent : MailboxProcessor<Request>) state =
    if memb = state.Local then
        let incarnation = if incarnation < state.Incarnation then state.Incarnation
                          else state.Incarnation + 1UL

        MembershipEvent.Alive(state.Local, incarnation) |> disseminate state
        Some { state with Incarnation = incarnation }
    else
        maybe {
            let! status = state.Members |> Map.tryFind memb
            let! newStatus =
                match status with
                | Alive i when incarnation >= i -> Some(Suspected incarnation)
                | Suspected i when incarnation > i -> Some(Suspected incarnation)
                | _ -> None

            suspicionTimeout state |> agent.PostAfter(Kill(memb, incarnation))
            return updateMembers memb newStatus state
        }

let private death memb incarnation state =
    if memb = state.Local then None
    else
        maybe {
            let! status = state.Members |> Map.tryFind memb
            match status with
            | Alive i
            | Suspected i when incarnation >= i ->
                return updateMembers memb (Dead incarnation) state
            | _ -> ()
        }

type MemberList = 
    private { Agent : Agent<Request> }
    with
        member x.Alive memb incarnation =
            Revive(memb, incarnation) |> x.Agent.Post
        member x.Suspect memb incarnation =
            Suspect(memb, incarnation) |> x.Agent.Post
        member x.Dead memb incarnation =
            Kill(memb, incarnation) |> x.Agent.Post
        member x.Members() = 
            x.Agent.PostAndReply Members
        member x.Length with get() = x.Agent.PostAndReply Length

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module MemberList =
    let createWith disseminator (config : Config) local members = 
        let rec handle (box : Agent<Request>) (state : State) = async {
            let! msg = box.Receive()
            let state' = 
                match msg with
                | Revive(m, i) -> revive m i state
                | Suspect(m, i) -> suspect m i box state
                | Kill(m, i) -> death m i state
                | Members(rc) -> 
                    state.Members |> Map.toList
                                  |> List.map (function | m, Alive i | m, Suspected i | m, Dead i -> m, i)
                                  |> rc.Reply
                    None
                | Length(rc) -> 
                    rc.Reply(state.Members.Count)
                    None
                |> getOrElse state

            return! handle box state'
        }

        let members' = members |> List.map (fun m -> m, Alive 0UL)
                               |> Map.ofList

        let state = 
            { Local = local
              Incarnation = 0UL
              Members = members'
              PeriodTimeout = config.PeriodTimeout
              DeadMembers = Set.empty
              Disseminator = disseminator }
        
        { Agent = Agent<Request>.Start(fun box -> handle box state) }

    let create disseminator config local = createWith disseminator config local []

    let makeMember host port =
        let ipAddress =
            Dns.GetHostAddresses(host)
            |> Array.filter (fun a -> a.AddressFamily = AddressFamily.InterNetwork)
            |> Array.head
        
        { IPAddress = ipAddress.GetAddressBytes() |> toInt64
          Port = port }

    let makeLocal port =
        let hostName = Dns.GetHostName()
        makeMember hostName port
