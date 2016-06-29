namespace SwimProtocol

open System
open System.Net

type Agent<'a> = MailboxProcessor<'a>

type IncarnationNumber = uint64

type PeriodSeqNumber = uint64

[<StructuralEquality; StructuralComparison>]
type Node =
    { IPAddress : int64 
      Port : uint16 }
    with
        override x.ToString() = string x.IPAddress + ":" + string x.Port

type Ping = PeriodSeqNumber
type PingRequest = PeriodSeqNumber * Node
type Ack = PeriodSeqNumber * Node

type Message = 
    | PingMessage of Ping
    | PingRequestMessage of PingRequest
    | AckMessage of Ack

type MembershipEvent = 
| Alive of Node * IncarnationNumber
| Suspect of Node * IncarnationNumber
| Dead of Node * IncarnationNumber

type SwimEvent =
| MembershipEvent of MembershipEvent
| UserEvent of string

type Config = 
    { Port: uint16
      Local : Node
      PeriodTimeout : TimeSpan
      PingTimeout: TimeSpan
      PingRequestGroupSize : int
      SuspectTimeout : TimeSpan }