#r "../../packages/Rx-Core/lib/net45/System.Reactive.Core.dll"
#r "../../packages/Rx-Interfaces/lib/net45/System.Reactive.Interfaces.dll"
#r "../../packages/Rx-Linq/lib/net45/System.Reactive.Linq.dll"
#r "../../packages/FSharp.Control.Reactive/lib/net45/FSharp.Control.Reactive.dll"
#r "../../packages/FSharpx.Collections/lib/net40/FSharpx.Collections.dll"
#r "../../packages/FSharp.Control.AsyncSeq/lib/net45/FSharp.Control.AsyncSeq.dll"
#r "../../packages/FSharpx.Async/lib/net40/FSharpx.Async.dll"
#r "../../packages/FSharpx.Extras/lib/40/FSharpx.Extras.dll"
#load "../../paket-files/MaxDeg/fs-msgpack/src/MsgPack/DataType.fs"
#load "../../paket-files/MaxDeg/fs-msgpack/src/MsgPack/Value.fs"
#load "../../paket-files/MaxDeg/fs-msgpack/src/MsgPack/Packer.fs"
#load "../../paket-files/MaxDeg/fs-msgpack/src/MsgPack/Unpacker.fs"
#load "Utils.fs"
#load "Types.fs"
#load "Message.fs"
#load "Transport.fs"
#load "Dissemination.fs"
#load "Membership.fs"
#load "FailureDetection.fs"
#load "Swim.fs"

open SwimProtocol
open System

let localName = System.Net.Dns.GetHostName()

let node1 =
    Swim.init { Swim.defaultConfig with Port = 1337us
                                        PeriodTimeout = TimeSpan.FromSeconds(1.) } []

let node2 =
    Swim.init { Swim.defaultConfig with Port = 1338us
                                        PeriodTimeout = TimeSpan.FromSeconds(1.) } [ (localName, 1337us) ]

let node3 =
    Swim.init { Swim.defaultConfig with Port = 1339us
                                        PeriodTimeout = TimeSpan.FromSeconds(1.) } [ (localName, 1337us) ]

// -------------------------------------------------------------
type Agent<'a> = MailboxProcessor<'a>

[<StructuralEquality; StructuralComparison>]
type Node =
    { IPAddress : int64
      Port : uint16 }
    override x.ToString() = string x.Port

type IncarnationNumber =
    | IncarnationNumber of uint64

type SeqNumber =
    | SeqNumber of uint64

type NodeStatus =
    | Alive of IncarnationNumber
    | Suspect of IncarnationNumber
    | Dead of IncarnationNumber

type SwimEvent =
    | Membership of Node * NodeStatus
    | User of string

type SwimMessage =
    | Ping of SeqNumber
    | PingRequest of SeqNumber * Node
    | Ack of SeqNumber * Node

[<RequireQualifiedAccess>]
module Sequence =
    let make() = SeqNumber(0UL)
    let incr = function
        | SeqNumber s -> SeqNumber(s + 1UL)

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module IncarnationNumber =
    let make() = IncarnationNumber(0UL)
    let incr = function
        | IncarnationNumber i -> IncarnationNumber(i + 1UL)

module Disseminate =
    type Disseminator = private { T: obj }

    // membership : Disseminator -> Node -> NodeStatus -> unit
    let membership disseminator node status = ()

    // user : Disseminator -> string -> unit
    let user disseminator event = ()


module Membership =
    type MemberList =
        private { Local : Node
                  Incarnation : IncarnationNumber
                  Members : Map<Node, NodeStatus>
                  PeriodTimeout : TimeSpan }

    type private FindResult =
        | Local of IncarnationNumber
        | Member of NodeStatus
        | NotFound

    // disseminate : MemberList -> Node -> NodeStatus -> unit
    let private disseminate memberList memb status =
        ()

    // tryFind : MemberList -> Node -> FindResult
    let private tryFind memberList memb =
        let { Local = local
              Incarnation = currentIncarnation
              Members = members } = memberList

        if memb = local then
            Local(currentIncarnation)
        else
            match Map.tryFind memb members with
            | Some s -> Member(s)
            | None -> NotFound

    // updateMembers : MemberList -> Node -> NodeStatus -> MemberList
    let private updateMembers memberList memb status =
        let { Members = members } = memberList
        let members' = Map.add memb status members
        // disseminate information
        disseminate memberList memb status

        { memberList with Members = members' }

    // stillAlive : MemberList -> IncarnationNumber -> MemberList
    let private stillAlive memberList incarnation =
        let { Local = local
              Incarnation = currentIncarnation } = memberList
        let incarnation' =  if currentIncarnation > incarnation then currentIncarnation
                            else IncarnationNumber.incr currentIncarnation
        // disseminate information
        disseminate memberList local (Alive incarnation')

        { memberList with Incarnation = incarnation' }

    // alive : MemberList -> Node -> IncarnationNumber -> MemberList
    let private alive memberList memb incarnation =
        match tryFind memberList memb with
        | NotFound ->
            let status = IncarnationNumber.make() |> Alive
            updateMembers memberList memb status

        | Member(Alive i)
        | Member(Suspect i) when i < incarnation ->
            updateMembers memberList memb (Alive incarnation)

        | Member _ -> memberList
        | Local _ -> memberList

    // suspect : MemberList -> Node -> IncarnationNumber -> MemberList
    let private suspect memberList memb incarnation =
        match tryFind memberList memb with
        | Local i -> stillAlive memberList i

        | Member(Alive i) when incarnation >= i ->
            updateMembers memberList memb (Suspect incarnation)

        | Member(Suspect i) when incarnation > i ->
            updateMembers memberList memb (Suspect incarnation)

        | Member _ -> memberList
        | NotFound -> memberList

    // dead : MemberList -> Node -> IncarnationNumber -> MemberList
    let private dead memberList memb incarnation =
        match tryFind memberList memb with
        | Local i -> stillAlive memberList i

        | Member(Alive i)
        | Member(Suspect i) when incarnation >= i ->
            updateMembers memberList memb (Dead i)

        | Member(Dead i) when incarnation > i ->
            updateMembers memberList memb (Dead i)

        | Member _ -> memberList
        | NotFound -> memberList

    // make : Node -> TimeSpan -> Node list -> MemberList
    let make local periodTimeout members =
        { Local = local
          Incarnation = IncarnationNumber.make()
          Members = members
          PeriodTimeout = periodTimeout }

    // update : MemberList -> Node -> Status -> MemberList
    let update memberList node = function
        | Alive i -> alive memberList node i
        | Suspect i -> suspect memberList node i
        | Dead i -> dead memberList node i

module FailureDetection =
    // ping : SeqNumber -> Async<bool>
    // pingRequest : SeqNumber -> Node -> Async<bool>
    // ack : Node -> SeqNumber -> unit

module Swim =
    type Config =
        { Port : uint16
          PeriodTimeout : TimeSpan
          PingTimeout : TimeSpan
          PingRequestGroupSize : int
          SuspectTimeout : TimeSpan }
