module SwimProtocol.Transport

open System
open System.IO
open System.Net
open System.Net.Sockets
open System.Runtime.Serialization.Formatters.Binary

type TransportMessage = IPEndPoint * byte []

type Socket = 
    private
    | Socket of UdpClient * IObservable<TransportMessage>

module private Observable = 
    [<CustomEquality; CustomComparison>]
    type private Subscriber = 
        | Subscriber of IObserver<TransportMessage>
        override x.Equals other = Object.Equals(x, other)
        override x.GetHashCode() = hash x
        interface IComparable with
            member x.CompareTo other = 
                match other with
                | :? Subscriber as obs -> 
                    if Object.Equals(x, obs) then 0
                    else 1
                | _ -> invalidArg "other" "cannot compare values of different types"
    
    type private AgentRequest = 
        | Subscribe of Subscriber
        | UnSubscribe of Subscriber
        | Send of TransportMessage
    
    let private handle observers = 
        function 
        | Subscribe obs -> Set.add obs observers
        | UnSubscribe obs -> Set.remove obs observers
        | Send msg -> 
            Set.iter (fun (Subscriber obs) -> obs.OnNext msg) observers
            observers
    
    let private agent = 
        MailboxProcessor<AgentRequest>.Start(fun inbox -> 
            let rec loop observers = async { let! msg = inbox.Receive()
                                             return! handle observers msg |> loop }
            loop Set.empty)
    
    let private unsubscribe obs = UnSubscribe obs |> agent.Post
    
    let subscribe obs = 
        let observer = Subscriber obs
        Subscribe observer |> agent.Post
        { new IDisposable with
              member __.Dispose() = unsubscribe observer }
    
    let send msg = Send msg |> agent.Post

let private listener (udp : UdpClient) callback = 
    async { 
        while true do
            let! msg = udp.ReceiveAsync() |> Async.AwaitTask
            callback (msg.RemoteEndPoint, msg.Buffer)
    }

let create port = 
    let udp = new UdpClient(port = port)
    
    let observable = 
        { new IObservable<TransportMessage> with
              member x.Subscribe obs = Observable.subscribe obs }
    listener udp Observable.send |> Async.Start
    Socket(udp, observable)

let send (Socket(udp, _)) addr buffer = 
    udp.SendAsync(buffer, buffer.Length, addr)
    |> Async.AwaitTask
    |> Async.Ignore

let receive (Socket(_, observable)) = observable

let private serialize msg = 
    let binFormatter = new BinaryFormatter()
    use stream = new MemoryStream()
    binFormatter.Serialize(stream, msg)
    stream.ToArray()

let private encodeMember m = ()
let private encodeEvents events = ()
let private encodePing p = [||]
let private encodePingRequest p = [||]
let private encodeAck p = [||]

let private decodeMember bytes = ()
let private decodeEvents events = ()
let private decodePing bytes = None
let private decodePingRequest bytes = None
let private decodeAck bytes = None

let encode msg : byte[] = 
    match msg with
    | Ping s -> encodePing msg
    | PingRequest(s, m) -> encodePingRequest msg
    | Ack(s, m) -> encodeAck msg

let decode bytes: DetectionMessage option = 
    [ decodeAck
      decodePing
      decodePingRequest ]
    |> List.tryPick (fun f -> f bytes)
