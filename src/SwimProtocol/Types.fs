namespace SwimProtocol

open System
open System.Net

type Agent<'a> = MailboxProcessor<'a>

[<RequireQualifiedAccess>]
module Agent =
    let spawn (state : 's) (handler : Agent<'a> -> 's -> 'a -> 's) =
        Agent<'a>.Start(fun box ->
            let rec loop state' = async {
                let! msg = box.Receive()
                return! handler box state' msg |> loop
            }

            loop state)

    let post (agent : Agent<'a>) msg =
        agent.Post msg
        
    let postAfter (agent : Agent<'a>) msg (timeout : TimeSpan) =
        Async.Start(async { 
            do! Async.Sleep(timeout.TotalMilliseconds |> int)
            agent.Post msg
        })
        
    let postAndReply (agent : Agent<'a>) msg =
        agent.PostAndReply msg

type SeqNumber =
    private | SeqNumber of uint64

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
