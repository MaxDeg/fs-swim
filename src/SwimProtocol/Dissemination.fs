module SwimProtocol.Dissemination

type private Request =
| Push of SwimEvent
| Pull of int * int * AsyncReplyChannel<MsgPack.Values.Value[]>
    
type private PiggyBackedEvent = SwimEvent * int
type private State = PiggyBackedEvent list

type EventDisseminator =
    private { Agent : MailboxProcessor<Request> }
    with
        member x.Push msg =
            Push msg |> x.Agent.Post

        member x.Pull numMembers maxSize =
            (fun rc -> Pull(numMembers, maxSize, rc))
            |> x.Agent.PostAndReply

let private push event state =
    (event, 0) :: state

let private maxPiggyBack numMembers =
    3. * round(log (float numMembers + 1.)) |> int

let private sortCompare (e1, cnt1) (e2, cnt2) =
    let eventOrder = function MembershipEvent _ -> 0 | UserEvent _ -> 1
    compare (eventOrder e1) (eventOrder e2) + (cnt1 - cnt2)

let private pull numMembers maxSize state =
    if numMembers = 0 then [||], state
    else
        let maxPiggyBack = maxPiggyBack numMembers
        let sizeOf = Message.encodeEvent >> Message.sizeOfValues

        let rec peek acc size = function
        | (evt, inc)::tail ->
            let size' = size - (sizeOf evt)
            if size' >= 0 then peek ((evt, inc + 1) :: acc) size' tail
            else acc, tail
        | lst -> acc, lst

        let selectedEvents, restEvents = peek [] maxSize (state |> List.sortWith sortCompare)

        selectedEvents |> List.toArray
                       |> Array.collect (fst >> Message.encodeEvent),
        (selectedEvents |> List.filter (snd >> (>) maxPiggyBack)) @ restEvents

let create() =
    let agent = MailboxProcessor<Request>.Start(fun box ->
        let rec loop state = async {
            let! msg = box.Receive()
            let state' =
                match msg with
                | Push event -> push event state
                | Pull (numMembers, maxSize, replyChan) ->
                    let membs, state' = pull numMembers maxSize state
                    replyChan.Reply membs
                    state'
            
            return! loop state'
        }
        loop [])
    
    { Agent = agent }
