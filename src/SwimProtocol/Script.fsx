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

type SeqNumber =
    private | SeqNumber of uint64

[<RequireQualifiedAccess>]
module Sequence =
    let make() = SeqNumber(0UL)
    let incr = function
        | SeqNumber s -> SeqNumber(s + 1UL)

type IncarnationNumber =
    private | IncarnationNumber of uint64

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module IncarnationNumber =
    let make() = IncarnationNumber(0UL)
    let incr = function
        | IncarnationNumber i -> IncarnationNumber(i + 1UL)

[<StructuralEquality; StructuralComparison>]
type Node =
    { IPAddress : int64
      Port : uint16 }
    override x.ToString() = string x.Port

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
    | Leave

type Udp =
    private { Client : System.Net.Sockets.UdpClient
              ReceivedEvent : Event<Node * byte[]> }

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Udp =
    open System.Net
    open System.Net.Sockets
    open System.Net.NetworkInformation

    [<Literal>]
    let MaxSize = 512
    
    let connect port = 
        let udp = new UdpClient(port = port)
        let receivedEvent = new Event<Node * byte[]>()

        Async.Start(async {
            while true do
                let! udpMsg = udp.ReceiveAsync() |> Async.AwaitTask
                let node = { IPAddress = udpMsg.RemoteEndPoint.Address.GetAddressBytes() |> toInt64
                             Port = uint16 udpMsg.RemoteEndPoint.Port }
                receivedEvent.Trigger(node, udpMsg.Buffer)
        })
        
        { Client = udp
          ReceivedEvent = receivedEvent }
    
    let asyncSend node bytes { Client = udp } =
        udp.SendAsync(bytes, bytes.Length, new IPEndPoint(node.IPAddress, int node.Port))
        |> Async.AwaitTask
        |> Async.Ignore

    let received { ReceivedEvent = receivedEvent } = receivedEvent.Publish


    let randomPort() =
        let isUdpPortUsed =
            let usedUdpPort =
                IPGlobalProperties.GetIPGlobalProperties().GetActiveUdpListeners() 
                |> Array.map (fun e -> e.Port)

            fun p -> Array.exists (fun p' -> p' = p) usedUdpPort
        
        let rec nextFreePort port =
            if isUdpPortUsed port then nextFreePort port
            else port 

        nextFreePort 1337


type Disseminator = private { Events : Map<SwimEvent, uint32> }

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Disseminator =
    let private add event disseminator =
        match Map.tryFind event disseminator.Events with
        | Some _ -> disseminator
        | None -> { disseminator with Events = Map.add event 0u disseminator.Events }
        
    let private maxPiggyBack numMembers =
        if numMembers = 0 then 0
        else
            3. * round(log (float numMembers + 1.)) |> int

    let private sortCompare (e1, cnt1) (e2, cnt2) =
        let eventOrder = function Membership _ -> 0 | User _ -> 1
        compare (eventOrder e1) (eventOrder e2) + (cnt1 - cnt2)

    // make : () -> Disseminator
    let make () = { Events = Map.empty }

    // membership : Node -> NodeStatus -> Disseminator -> Disseminator
    let membership node status = Membership(node, status) |> add

    // user : string -> Disseminator -> Disseminator
    let user event = User event |> add

    let take numMembers maxSize disseminator =
        let maxPiggyBack = maxPiggyBack numMembers

        [], disseminator

type MemberList =
    private { Local : Node
              Incarnation : IncarnationNumber
              Members : Map<Node, NodeStatus>
              PeriodTimeout : TimeSpan }

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module MemberList =
    type private FindResult =
        | Local of IncarnationNumber
        | Member of NodeStatus
        | NotFound

    // disseminate : MemberList -> Node -> NodeStatus -> unit
    let private disseminate memb status memberList =
        ()

    // tryFind : MemberList -> Node -> FindResult
    let private tryFind memb memberList =
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
    let private updateMembers memb status memberList =
        let { Members = members } = memberList
        let members' = Map.add memb status members
        // disseminate information
        disseminate memberList memb status

        { memberList with Members = members' }

    // stillAlive : MemberList -> IncarnationNumber -> MemberList
    let private stillAlive incarnation memberList =
        let { Local = local
              Incarnation = currentIncarnation } = memberList
        let incarnation' =  if currentIncarnation > incarnation then currentIncarnation
                            else IncarnationNumber.incr currentIncarnation
        // disseminate information
        disseminate memberList local (Alive incarnation')

        { memberList with Incarnation = incarnation' }

    // alive : MemberList -> Node -> IncarnationNumber -> MemberList
    let private alive memb incarnation memberList =
        match tryFind memb memberList with
        | NotFound ->
            let status = IncarnationNumber.make() |> Alive
            updateMembers memb status memberList

        | Member(Alive i)
        | Member(Suspect i) when i < incarnation ->
            updateMembers memb (Alive incarnation) memberList

        | Member _ -> memberList
        | Local _ -> memberList

    // suspect : MemberList -> Node -> IncarnationNumber -> MemberList
    let private suspect memb incarnation memberList =
        match tryFind memb memberList with
        | Local i -> stillAlive i memberList

        | Member(Alive i) when incarnation >= i ->
            updateMembers memb (Suspect incarnation) memberList

        | Member(Suspect i) when incarnation > i ->
            updateMembers memb (Suspect incarnation) memberList

        | Member _ -> memberList
        | NotFound -> memberList

    // dead : MemberList -> Node -> IncarnationNumber -> MemberList
    let private dead memb incarnation memberList =
        match tryFind memb memberList with
        | Local i -> stillAlive i memberList

        | Member(Alive i)
        | Member(Suspect i) when incarnation >= i ->
            updateMembers memb (Dead i) memberList

        | Member(Dead i) when incarnation > i ->
            updateMembers memb (Dead i) memberList

        | Member _ -> memberList
        | NotFound -> memberList

    // make : Node -> TimeSpan -> Node list -> MemberList
    let make local periodTimeout members =
        { Local = local
          Incarnation = IncarnationNumber.make()
          Members = members
          PeriodTimeout = periodTimeout }

    // update : MemberList -> Node -> Status -> MemberList
    let update node status memberList =
        match status with
        | Alive i -> alive node i memberList
        | Suspect i -> suspect node i memberList
        | Dead i -> dead node i memberList

type FailureDetector =
    private { Timeout : TimeSpan
              AsyncSendMessage : Node -> SwimMessage -> Async<unit>
              AsyncReceiveMessageFor : (Node -> SwimMessage -> bool) -> TimeSpan -> Async<SwimMessage option> }

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module FailureDetector =
    // ackFilter : Node -> SeqNumber -> Node -> SwimMessage -> bool
    let private ackFilter node seqNr = fun _ -> 
        function Ack(s, n) -> seqNr = s && node = n | _ -> false

    // sendMessage : FailureDetector -> Node -> SwimMessage
    let private sendMessage detector node message = async {
        do! detector.AsyncSendMessage node message
        return None
    }

    // make : TimeSpan -> (Node -> SwimMessage -> Async<unit>) -> ((Node -> SwimMessage -> bool) -> TimeSpan -> Async<SwimMessage option>) -> FailureDetector
    let make timeout asyncSendMessage asyncReceiveMessageFor =
        { Timeout = timeout
          AsyncSendMessage = asyncSendMessage
          AsyncReceiveMessageFor = asyncReceiveMessageFor }

    // ping : SeqNumber -> Node -> FailureDetector -> Async<bool>
    let ping seqNr node detector = async {
        let! results =
            [ detector.AsyncReceiveMessageFor (ackFilter node seqNr) detector.Timeout
              Ping seqNr |> sendMessage detector node ]
            |> Async.Parallel

        return Array.head results |> Option.isSome
    }

    // pingRequest : SeqNumber -> Node -> Node list -> FailureDetector -> Async<bool>
    let pingRequest seqNr node nodes detector = async {
        let messages = nodes |> List.map (fun n -> PingRequest(seqNr, node) |> sendMessage detector n)

        let! results =
            detector.AsyncReceiveMessageFor (ackFilter node seqNr) detector.Timeout :: messages
            |> Async.Parallel

        return Array.head results |> Option.isSome
    }

    // ack : SeqNumber -> Node -> Node -> FailureDetector -> unit
    let ack seqNr node replyNode detector = 
        Ack(seqNr, node) |> detector.AsyncSendMessage replyNode


module Swim =
    type Config =
        { Port : uint16
          PeriodTimeout : TimeSpan
          PingTimeout : TimeSpan
          PingRequestGroupSize : int
          SuspectTimeout : TimeSpan }

    // make : unit -> IDisposable
    
    // handle : Node -> SwimMessage -> detector
    (*let handle source message detector =
        match message with
        | Ping seqNr ->
            ack seqNr detector.Local source detector
        | PingRequest(seqNr, node) -> async {
            let! acked = ping seqNr node detector
            if acked then
                do! ack seqNr node source detector }
        | Ack _ -> async { () }*)
