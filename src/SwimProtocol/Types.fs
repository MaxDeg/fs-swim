namespace SwimProtocol

open System
open System.Threading

type Agent<'a> = MailboxProcessor<'a>

[<RequireQualifiedAccess>]
module Agent =
    let spawn (state : 's) handler =
        let rec handleLoop state' (agent : Agent<'a>) = async {
            try
                let! msg = agent.Receive()
                let state' = handler agent state' msg
                return! handleLoop state' agent
            with e ->
                printfn "Error!! %A" e
                return! handleLoop state' agent
        }
        
        Agent<'a>.Start(handleLoop state)
    
    let post (agent : Agent<'a>) =
        agent.Post
        
    let postAfter (agent : Agent<'a>) msg (timeout : TimeSpan) =
        Async.Start(async { 
            do! Async.Sleep(timeout.TotalMilliseconds |> int)
            agent.Post msg
        })
        
    let postAndReply (agent : Agent<'a>) =
        agent.PostAndReply

type SeqNumber =
    private | SeqNumber of uint64
    with
        override x.ToString() = 
            match x with SeqNumber i -> i.ToString()

[<RequireQualifiedAccess>]
module Sequence =
    let make() = SeqNumber(0UL)
    let makeFrom value = SeqNumber(value)
    let incr = function
        | SeqNumber s -> SeqNumber(s + 1UL)
        
    let (|Number|) = function
        | SeqNumber s -> s 

type IncarnationNumber =
    private | IncarnationNumber of uint64
    with
        override x.ToString() = 
            match x with IncarnationNumber i -> i.ToString()

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module IncarnationNumber =
    let make() = IncarnationNumber(0UL)
    let makeFrom value = IncarnationNumber(value)
    let incr = function
        | IncarnationNumber i -> IncarnationNumber(i + 1UL)
        
    let (|Number|) = function
        | IncarnationNumber i -> i

[<StructuralEquality; StructuralComparison>]
type Node =
    { IPAddress : int64
      Port : uint16 }
    override x.ToString() = string x.Port

type LocalNode = Node * IncarnationNumber

type NodeStatus =
    | Alive of IncarnationNumber
    | Suspect of IncarnationNumber
    | Dead of IncarnationNumber

[<StructuralEquality; StructuralComparison>]
type SwimEvent =
    | Membership of Node * NodeStatus
    | User of string

type SwimMessage =
    | Ping of SeqNumber
    | PingRequest of SeqNumber * Node
    | Ack of SeqNumber * Node
    | Leave of IncarnationNumber
