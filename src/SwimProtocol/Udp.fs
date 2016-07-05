namespace SwimProtocol

type Udp =
    private { Client : System.Net.Sockets.UdpClient
              ReceivedEvent : Event<Node * byte[]> }

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Udp =
    open System
    open System.Net
    open System.Net.Sockets
    open System.Net.NetworkInformation

    [<Literal>]
    let MaxSize = 512
    
    let connect (port : UInt16) = 
        let udp = new UdpClient(port = int port)
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
    
    let send node bytes { Client = udp } =
        udp.Send(bytes, bytes.Length, new IPEndPoint(node.IPAddress, int node.Port))
        |> ignore

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