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

let private tryRevive status nextIncarnation = 
    match status with
    | Alive i | Suspected i when nextIncarnation > i -> Some(Alive nextIncarnation)
    | Alive i | Suspected i | Dead i -> None

let private trySuspect status nextIncarnation = 
    match status with
    | Alive i when nextIncarnation >= i -> Some(Suspected nextIncarnation)
    | Suspected i when nextIncarnation > i -> Some(Suspected nextIncarnation)
    | _ -> None

type private Request = 
    | Revive of Node * IncarnationNumber
    | Suspect of Node * IncarnationNumber
    | Kill of Node * IncarnationNumber
    | Members of AsyncReplyChannel<(Node * IncarnationNumber) list>
    | Length of AsyncReplyChannel<int>

type private State = 
    { Members : Map<Node, MemberStatus>
      SuspectTimeout: TimeSpan
      DeadMembers : Set<Node>
      Disseminator : EventDisseminator }

let private disseminate memb status state =
    let event = 
        match status with
        | Alive i -> MembershipEvent.Alive(memb, i)
        | Suspected i -> MembershipEvent.Suspect(memb, i)
        | Dead i -> MembershipEvent.Dead(memb, i)
    state.Disseminator.Push(MembershipEvent event)

let private updateMembers memb status state =
    disseminate memb status state
    match status with
    | Dead _ -> 
        { state with Members = state.Members |> Map.remove memb
                     DeadMembers = state.DeadMembers |> Set.add memb }
    | _ -> 
        { state with Members = state.Members |> Map.add memb status }

let private revive memb incarnation state = 
    maybe {
        let status = state.Members |> Map.tryFind memb 
                                   |> getOrElse (Alive 0UL)
        let! newStatus = tryRevive status incarnation
        return updateMembers memb newStatus state
    }

let private suspect memb incarnation (agent : MailboxProcessor<Request>) state = 
    maybe { 
        let! status = state.Members |> Map.tryFind memb
        let! newStatus = trySuspect status incarnation
        state.SuspectTimeout |> agent.PostAfter(Kill(memb, incarnation))
        return updateMembers memb newStatus state
    }

let private death memb incarnation state = 
    maybe {
        let! status = state.Members |> Map.tryFind memb
        match status with
        | Suspected i when i <= incarnation -> return updateMembers memb (Dead incarnation) state
        | _ -> ()
    }

type MemberList = 
    private { Agent : MailboxProcessor<Request> }
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
    let createWith disseminator suspectTimeout members = 
        let rec handle (box : MailboxProcessor<Request>) (state : State) = async {
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

        let state = 
            { Members = Map.empty
              SuspectTimeout = suspectTimeout
              DeadMembers = Set.empty
              Disseminator = disseminator }

        let state' = List.fold (fun s m -> updateMembers m (Alive 1UL) s) state members
        
        { Agent = MailboxProcessor<Request>.Start(fun box -> handle box state') }

    let create suspectTimeout disseminator = createWith disseminator suspectTimeout []

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
