module SwimProtocol.Transport

open System
open System.Net
open System.Net.Sockets
open FSharp.Control
open FSharpx.Option

type TransportMessage<'a> = IPEndPoint * 'a

type ISerializer<'a> =
    abstract member Serialize : 'a -> byte[]
    abstract member Deserialize : byte[] -> 'a option

type Transport<'a> =
    private { Client : UdpClient
              Serializer : ISerializer<'a>
              MessageSource : AsyncSeqSrc<TransportMessage<'a>> }
    with
        member x.Send addr msg =
            let bytes = x.Serializer.Serialize msg
            x.Client.SendAsync(bytes, bytes.Length, addr)
            |> Async.AwaitTask
            |> Async.Ignore

        member x.Receive() = AsyncSeqSrc.toAsyncSeq x.MessageSource

let create port (serializer : ISerializer<'a>) = 
    let udp = new UdpClient(port = port)  
    let source = AsyncSeqSrc.create()

    Async.Start(async {
        while true do
            let! updMsg = udp.ReceiveAsync() |> Async.AwaitTask
            maybe {
                let! msg = serializer.Deserialize updMsg.Buffer
                AsyncSeqSrc.put (updMsg.RemoteEndPoint, msg) source
            } |> ignore
    })
    
    { Client = udp
      Serializer = serializer
      MessageSource = source }
