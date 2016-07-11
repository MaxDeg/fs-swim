namespace SwimProtocol

open System
open System.Threading

type Agent<'a> = 
    private { Mailbox : MailboxProcessor<'a>
              CancellationSource : CancellationTokenSource }

[<RequireQualifiedAccess>]
module Agent =
    let spawn (state : 's) handler =
        let cancellationTokenSource = new CancellationTokenSource()

        let rec handleLoop state' agent = async {
            try
                let! msg = agent.Mailbox.Receive()
                let state' = handler agent state' msg
                return! handleLoop state' agent
            with e ->
                printfn "Error!! %A" e
                return! handleLoop state' agent
        }
        
        let rec agent =
            { Mailbox = MailboxProcessor<'a>.Start((fun _ -> handleLoop state agent), cancellationTokenSource.Token)
              CancellationSource = cancellationTokenSource }
        agent

    let stop { CancellationSource = cancelSource } =
        cancelSource.Cancel()

    let post { Mailbox = agent } =
        agent.Post
        
    let postAfter { Mailbox = agent } msg (timeout : TimeSpan) =
        Async.Start(async { 
            do! Async.Sleep(timeout.TotalMilliseconds |> int)
            agent.Post msg
        })
        
    let postAndReply { Mailbox = agent } =
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
