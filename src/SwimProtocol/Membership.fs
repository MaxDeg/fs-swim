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
    | _ -> None

let private trySuspect status nextIncarnation = 
    match status with
    | Alive i when nextIncarnation >= i -> Some(Suspected nextIncarnation)
    | Suspected i when nextIncarnation > i -> Some(Suspected nextIncarnation)
    | _ -> None

type private Request = 
    | Revive of Member * IncarnationNumber
    | Suspect of Member * IncarnationNumber
    | Kill of Member * IncarnationNumber
    | Members of AsyncReplyChannel<(Member * IncarnationNumber) list>

type private State = 
    { Members : Map<Member, MemberStatus>
      SuspectTimeout: TimeSpan
      DeadMembers : Set<Member>
      Disseminator : EventDisseminator }

    with
        member x.Disseminate memb status =
            let event = 
                match status with
                | Alive i -> MembershipEvent.Alive(memb, i)
                | Suspected i -> MembershipEvent.Suspect(memb, i)
                | Dead i -> MembershipEvent.Dead(memb, i)
            x.Disseminator.Push(MembershipEvent event)

        member x.UpdateMembers memb status =
            x.Disseminate memb status    
            match status with
            | Dead _ -> 
                { x with Members = x.Members |> Map.remove memb
                         DeadMembers = x.DeadMembers |> Set.add memb }
            | _ -> 
                { x with Members = x.Members |> Map.add memb status }

        member x.Revive memb incarnation = 
            maybe {
                let status = x.Members |> Map.tryFind memb |> getOrElse (Alive incarnation)
                let! newStatus = tryRevive status incarnation
                return x.UpdateMembers memb newStatus
            }

        member x.Suspect memb incarnation (agent : MailboxProcessor<Request>) = 
            maybe { 
                let! status = x.Members |> Map.tryFind memb
                let! newStatus = trySuspect status incarnation
                x.SuspectTimeout |> agent.PostAfter(Kill(memb, incarnation))
                return x.UpdateMembers memb newStatus
            }

        member x.Death memb incarnation = 
            maybe {
                let! status = x.Members |> Map.tryFind memb
                match status with
                | Suspected i when i <= incarnation -> return x.UpdateMembers memb (Dead incarnation)
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

let createWith disseminator suspectTimeout members = 
    let rec handle (box : MailboxProcessor<Request>) (state : State) = async {
        let! msg = box.Receive()
        let state' = 
            match msg with
            | Revive(m, i) -> state.Revive m i
            | Suspect(m, i) -> state.Suspect m i box
            | Kill(m, i) -> state.Death m i
            | Members(rc) -> 
                state.Members
                |> Map.toList
                |> List.map (function | m, Alive i | m, Suspected i | m, Dead i -> m, i)
                |> rc.Reply
                None
            |> getOrElse state

        return! handle box state'
    }
    
    let members' = members |> List.map (fun m -> m, Alive 0UL) |> Map.ofList
    let state = 
        { Members = members'
          SuspectTimeout = suspectTimeout
          DeadMembers = Set.empty
          Disseminator = disseminator }
    
    { Agent = MailboxProcessor<Request>.Start(fun box -> handle box state) }

let create suspectTimeout disseminator = createWith disseminator suspectTimeout []

let makeMember host port =
    let ipAddress =
        Dns.GetHostAddresses(host)
        |> Array.filter (fun a -> a.AddressFamily = AddressFamily.InterNetwork)
        |> Array.head

    { Name = sprintf "gossip:%s@%A:%i" host ipAddress port
      Address = new IPEndPoint(ipAddress, port) }

let makeLocal port =
    let hostName = Dns.GetHostName()
    makeMember hostName port
