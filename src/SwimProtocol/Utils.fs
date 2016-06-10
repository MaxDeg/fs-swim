namespace SwimProtocol

open System

[<AutoOpen>]
module Utils = 
    let choice list = List.tryPick id list
    
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

        /// Creates an asynchronous workflow that non-deterministically returns the 
        /// result of one of the two specified workflows (the one that completes
        /// first). This is similar to Task.WaitAny.
        static member Choose(a, b) : Async<'T> = 
            Async.FromContinuations(fun (cont, econt, ccont) ->
            // Results from the two 
            let result1 = ref (Choice1Of3())
            let result2 = ref (Choice1Of3())
            let handled = ref false
            let lockObj = new obj()
            let synchronized f = lock lockObj f

            // Called when one of the workflows completes
            let complete () = 
                let op =
                    synchronized (fun () ->
                    // If we already handled result (and called continuation)
                    // then ignore. Otherwise, if the computation succeeds, then
                    // run the continuation and mark state as handled.
                    // Only throw if both workflows failed.
                    match !handled, !result1, !result2 with 
                    | true, _, _ -> ignore
                    | false, (Choice2Of3 value), _ 
                    | false, _, (Choice2Of3 value) -> 
                        handled := true
                        (fun () -> cont value)
                    | false, Choice3Of3 e1, Choice3Of3 e2 -> 
                        handled := true; 
                        (fun () -> 
                            econt (new AggregateException
                                        ("Both clauses of a choice failed.", [| e1; e2 |])))
                    | false, Choice1Of3 _, Choice3Of3 _ 
                    | false, Choice3Of3 _, Choice1Of3 _ 
                    | false, Choice1Of3 _, Choice1Of3 _ -> ignore )
                op() 

            // Run a workflow and write result (or exception to a ref cell
            let run resCell workflow = async {
                try
                    let! res = workflow
                    synchronized (fun () -> resCell := Choice2Of3 res)
                with e ->
                    synchronized (fun () -> resCell := Choice3Of3 e)
                complete() }

            // Start both work items in thread pool
            Async.Start(run result1 a)
            Async.Start(run result2 b) )

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

module Observable = 
    open FSharp.Control.Reactive

    let await (timeout : TimeSpan) = 
        Observable.head
        >> Observable.map Some
        >> Observable.timeoutSpanOther timeout (Observable.single None)
