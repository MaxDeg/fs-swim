namespace FSharp.Control

open System
open System.Diagnostics
open System.IO
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open System.Runtime.ExceptionServices

type AsyncSeqSrc<'a> = private { tail : AsyncSeqSrcNode<'a> ref }

and private AsyncSeqSrcNode<'a> =
    val tcs : TaskCompletionSource<('a * AsyncSeqSrcNode<'a>) option>
    new (tcs) = { tcs = tcs }

module internal AsyncSeqSrcImpl =

    let private createNode () =
        new AsyncSeqSrcNode<_>(new TaskCompletionSource<_>())

    let create () : AsyncSeqSrc<'a> =
        { tail = ref (createNode ()) }
        
    let put (a:'a) (s:AsyncSeqSrc<'a>) =      
        let newTail = createNode ()
        let tail = Interlocked.Exchange(s.tail, newTail)
        tail.tcs.SetResult(Some(a, newTail))
        
    let close (s:AsyncSeqSrc<'a>) : unit =
        s.tail.Value.tcs.SetResult(None)

    let error (ex:exn) (s:AsyncSeqSrc<'a>) : unit =
        s.tail.Value.tcs.SetException(ex)

    let rec private toAsyncSeqImpl (s:AsyncSeqSrcNode<'a>) : AsyncSeq<'a> = 
        asyncSeq {
        let! next = s.tcs.Task |> Async.AwaitTask
        match next with
        | None -> ()
        | Some (a,tl) ->
            yield a
            yield! toAsyncSeqImpl tl }

    let toAsyncSeq (s:AsyncSeqSrc<'a>) : AsyncSeq<'a> =
        toAsyncSeqImpl s.tail.Value


module AsyncSeqSrc =
    
  let create () = AsyncSeqSrcImpl.create ()
  let put a s = AsyncSeqSrcImpl.put a s
  let close s = AsyncSeqSrcImpl.close s
  let toAsyncSeq s = AsyncSeqSrcImpl.toAsyncSeq s
  let error e s = AsyncSeqSrcImpl.error e s