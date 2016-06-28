module SwimProtocol.Transport

open System
open System.Net
open System.Net.NetworkInformation
open System.Net.Sockets
open FSharp.Control
open FSharpx.Option

type TransportMessage= Node * byte[]

type Udp =
    private { Client : UdpClient
              ReceivedEvent : Event<TransportMessage> }
    with
        member x.Received = x.ReceivedEvent.Publish

        member x.Send(node, bytes) =
            x.Client.Send(bytes, bytes.Length, new IPEndPoint(node.IPAddress, int node.Port)) |> ignore


[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Udp =
    [<Literal>]
    let maxSize = 512
    
    let connect port = 
        let udp = new UdpClient(port = port)
        let receivedEvent = new Event<TransportMessage>()

        Async.Start(async {
            while true do
                let! udpMsg = udp.ReceiveAsync() |> Async.AwaitTask
                let node = { IPAddress = udpMsg.RemoteEndPoint.Address.GetAddressBytes() |> toInt64
                             Port = uint16 udpMsg.RemoteEndPoint.Port }
                receivedEvent.Trigger(node, udpMsg.Buffer)
        })
        
        { Client = udp
          ReceivedEvent = receivedEvent }
    
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

