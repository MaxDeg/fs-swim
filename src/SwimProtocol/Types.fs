namespace SwimProtocol

open System
open System.Net

type Agent<'a> = MailboxProcessor<'a>

type SeqNumber =
    private | SeqNumber of uint64

[<RequireQualifiedAccess>]
module Sequence =
    let make() = SeqNumber(0UL)
    let makeFrom value = SeqNumber(value)
    let incr = function
        | SeqNumber s -> SeqNumber(s + 1UL)
        
    let (|SeqNumber|) = function
        | SeqNumber s -> s 

type IncarnationNumber =
    private | IncarnationNumber of uint64

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module IncarnationNumber =
    let make() = IncarnationNumber(0UL)
    let makeFrom value = IncarnationNumber(value)
    let incr = function
        | IncarnationNumber i -> IncarnationNumber(i + 1UL)
        
    let (|IncarnationNumber|) = function
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
