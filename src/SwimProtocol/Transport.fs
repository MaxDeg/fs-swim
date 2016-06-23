module SwimProtocol.Transport

open System
open System.Net
open System.Net.Sockets
open FSharp.Control
open FSharpx.Option

type TransportMessage= IPEndPoint * byte[]

type Transport =
    private { Client : UdpClient
              ReceivedEvent : Event<TransportMessage> }
    with
        member x.Received = x.ReceivedEvent.Publish

        member x.Send addr bytes =
            x.Client.SendAsync(bytes, bytes.Length, addr)
            |> Async.AwaitTask
            |> Async.Ignore

let create port = 
    let udp = new UdpClient(port = port)
    let receivedEvent = new Event<TransportMessage>()

    Async.Start(async {
        while true do
            let! updMsg = udp.ReceiveAsync() |> Async.AwaitTask
            receivedEvent.Trigger(updMsg.RemoteEndPoint, updMsg.Buffer)
    })
    
    { Client = udp
      ReceivedEvent = receivedEvent }
