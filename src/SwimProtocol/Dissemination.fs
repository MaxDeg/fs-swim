module SwimProtocol.Dissemination

type private Request =
| Push of SwimEvent
| Pull of int * AsyncReplyChannel<SwimEvent[]>
    
type private PiggyBackedEvent = SwimEvent * int
type private State = PiggyBackedEvent list

type EventDisseminator =
    private { Agent : MailboxProcessor<Request> }
    with
        member x.Push msg =
            Push msg |> x.Agent.Post

        member x.Pull numMembers =
            (fun rc -> Pull(numMembers, rc))
            |> x.Agent.PostAndReply

let private push event state =
    printfn "[Event push] %A" event
    (event, 0) :: state

let private pull numMembers state =
    if numMembers = 0 then Array.empty, state
    else
        let maxPiggyBack = 3. * round(log (float numMembers + 1.)) |> int
        let state' = List.filter (fun (_, c) -> c < maxPiggyBack) state
        let sortCompare (e1, cnt1) (e2, cnt2) =
            let eventOrder = function MembershipEvent _ -> 0 | UserEvent _ -> 1
            compare (eventOrder e1) (eventOrder e2) + (cnt1 - cnt2)
        
        state' |> List.sortWith sortCompare
               |> List.map fst
               |> List.toArray,
        state'

let create() =
    let agent = MailboxProcessor<Request>.Start(fun box ->
        let rec loop state = async {
            let! msg = box.Receive()
            let state' =
                match msg with
                | Push event -> push event state
                | Pull (numMembers, replyChan) ->
                    let membs, state' = pull numMembers state
                    replyChan.Reply membs
                    state'
            
            return! loop state'
        }
        loop List.empty)
    
    { Agent = agent }
