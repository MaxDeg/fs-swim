namespace SwimProtocol

open System

[<AutoOpen>]
module Utils = 
    let choice list = List.tryPick id list
    
    let toInt64 addr =
        match Array.length addr with
        | 4 -> BitConverter.ToInt32(addr, 0) |> int64
        | 8 -> BitConverter.ToInt64(addr, 0)
        | _ -> failwith "Incorrect format"
    
    type MailboxProcessor<'a> with
        member x.PostAfter msg (timespan : TimeSpan) = 
            Async.Start(async { 
                            do! Async.Sleep(timespan.TotalMilliseconds |> int)
                            x.Post msg
                        })
    
    type Async with
        static member Delay value (time : TimeSpan) = async {
            do! time.TotalMilliseconds |> int |> Async.Sleep
            return value
        }

module Array = 
    open System
    
    let swapInPlace i j arr = 
        let tmp = Array.item i arr
        Array.item j arr |> Array.set arr i
        Array.set arr j tmp
    
    let shuffle arr = 
        let rand = new Random()
        
        let rec shuffle' indexes = 
            function 
            | 0 -> indexes
            | i -> 
                let fst = rand.Next(i)
                swapInPlace i fst indexes
                shuffle' indexes (i - 1)
        shuffle' arr (Array.length arr)

module List = 
    open System
    
    let shuffle list = 
        let rand = new Random()
        
        let rec shuffle' indexes = 
            function
            | i when i > 0 -> 
                let fst = rand.Next(i)
                Array.swapInPlace i fst indexes
                shuffle' indexes (i - 1)
            | _ -> indexes
        
        let length = List.length list
        let shuffled = shuffle' [| 0..length - 1 |] (length - 1)
        List.permute (fun i -> shuffled.[i]) list

[<RequireQualifiedAccess>]
module Observable =
    open FSharp.Control.Reactive

    let timeoutSpanOption timeout =
        Observable.timeoutSpanOther timeout (Observable.result None)
