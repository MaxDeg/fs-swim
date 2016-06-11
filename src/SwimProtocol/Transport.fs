module SwimProtocol.Transport

open System
open System.IO
open System.Net
open System.Net.Sockets
open FSharp.Control

type TransportMessage<'a> = IPEndPoint * 'a

let create port = 
    let udp = new UdpClient(port = port)  
    
    let send addr bytes =
        printfn "Sending to %A" addr
        udp.SendAsync(bytes, bytes.Length, addr)
        |> Async.AwaitTask
        |> Async.Ignore
    
    let receive = asyncSeq {
        while true do
            let! msg = udp.ReceiveAsync() |> Async.AwaitTask
            printfn "Received: %A" msg
            yield msg.RemoteEndPoint, msg.Buffer
    }
    
    send, receive