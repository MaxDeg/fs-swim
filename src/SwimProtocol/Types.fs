namespace SwimProtocol

open System
open System.Net

type Agent<'a> = MailboxProcessor<'a>

type IncarnationNumber = uint64

type PeriodSeqNumber = uint64

[<CustomEquality; CustomComparison>]
type Member = 
    { Name : string
      Address : IPEndPoint }
    
    override x.Equals other = 
        match other with
        | :? Member as o -> x.Name = o.Name
        | _ -> false
    
    override x.GetHashCode() = hash x.Name
    interface IComparable with
        member x.CompareTo other = 
            match other with
            | :? Member as o -> compare x.Name o.Name
            | _ -> invalidArg "other" "cannot compare values of different types"

type MembershipEvent = 
| Alive of Member * IncarnationNumber
| Suspect of Member * IncarnationNumber
| Dead of Member * IncarnationNumber

type SwimEvent =
| MembershipEvent of MembershipEvent
| UserEvent of string
