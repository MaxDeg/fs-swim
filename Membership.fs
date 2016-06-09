module SwimProtocol.MemberList

open FSharpx.Option
open System
open System.Net.NetworkInformation
open System.Net
open System.Net.Sockets

type private MemberStatus = 
    | Alive of IncarnationNumber
    | Suspected of IncarnationNumber
    | Dead of IncarnationNumber

type private MemberList = 
    { Members : Map<Member, MemberStatus>
      DeadMembers : Set<Member>
      Disseminator : EventsDissemination.T }

type private Request = 
    | Revive of Member * IncarnationNumber
    | Suspect of Member * IncarnationNumber
    | Kill of Member * IncarnationNumber
    | Members of AsyncReplyChannel<(Member * IncarnationNumber) list>

type T = 
    private
    | T of MailboxProcessor<Request>

let private disseminate memb status { Disseminator = disseminator } = 
    let event = 
        match status with
        | Alive i -> MembershipEvent.Alive(memb, i)
        | Suspected i -> MembershipEvent.Suspect(memb, i)
        | Dead i -> MembershipEvent.Dead(memb, i)
    EventsDissemination.push disseminator event

let private tryRevive status nextIncarnation = 
    match status with
    | Alive i | Suspected i when nextIncarnation > i -> Some(Alive nextIncarnation)
    | _ -> None

let private trySuspect status nextIncarnation = 
    match status with
    | Alive i when nextIncarnation >= i -> Some(Suspected nextIncarnation)
    | Suspected i when nextIncarnation > i -> Some(Suspected nextIncarnation)
    | _ -> None

let private updateMembers memb status ({ Members = members; DeadMembers = deadMembers } as memberList) = 
    disseminate memb status memberList
    
    match status with
    | Dead _ -> 
        { memberList with Members = Map.remove memb members
                          DeadMembers = Set.add memb deadMembers }
    | _ -> 
        { memberList with Members = Map.add memb status members }

let private handleRevive memb incarnation ({ Members = members } as memberList) = 
    maybe { 
        let status = Map.tryFind memb members |> getOrElse (Alive incarnation)
        let! newStatus = tryRevive status incarnation
        return updateMembers memb newStatus memberList
    }

let private handleSuspect memb incarnation ({ Members = members } as memberList) (agent : MailboxProcessor<Request>) = 
    maybe { 
        let! status = Map.tryFind memb members
        let! newStatus = trySuspect status incarnation
        TimeSpan.FromMinutes 5.0 |> agent.PostAfter(Kill(memb, incarnation))
        return updateMembers memb newStatus memberList
    }

let private handleDeath memb incarnation ({ Members = members } as memberList) = 
    maybe { 
        let! status = Map.tryFind memb members
        match status with
        | Suspected i when i <= incarnation -> return updateMembers memb (Dead incarnation) memberList
        | _ -> ()
    }

let private handle agent memberList msg = 
    match msg with
    | Revive(m, i) -> handleRevive m i memberList
    | Suspect(m, i) -> handleSuspect m i memberList agent
    | Kill(m, i) -> handleDeath m i memberList
    | Members(rc) -> 
        memberList.Members
        |> Map.toList
        |> List.map (function | m, Alive i | m, Suspected i | m, Dead i -> m, i)
        |> rc.Reply
        None
    |> getOrElse memberList

let createWith disseminator members = 
    let members' = members |> List.map (fun m -> m, Alive 0UL) |> Map.ofList
    let memberList = 
        { Members = members'
          DeadMembers = Set.empty
          Disseminator = disseminator }
    
    let agent = new MailboxProcessor<Request>(fun box -> 
        let rec loop memberList = async { let! msg = box.Receive()
                                          return! handle box memberList msg |> loop }
        loop memberList)
    
    agent.Start()
    T agent

let create disseminator = createWith disseminator []
let alive (T agent) memb incarnation = Revive(memb, incarnation) |> agent.Post
let suspect (T agent) memb incarnation = Suspect(memb, incarnation) |> agent.Post
let dead (T agent) memb incarnation = Kill(memb, incarnation) |> agent.Post

let members (T agent) = 
    agent.PostAndReply Members

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
