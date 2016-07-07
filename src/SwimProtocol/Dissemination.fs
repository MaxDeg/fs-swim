module SwimProtocol.Dissemination

open MsgPack

type private Request =
    | Event of SwimEvent
    | Take of int * int * AsyncReplyChannel<Values.Value[]>

type Dissemination = private Dissemination of Agent<Request>

[<RequireQualifiedAccess; AutoOpen>]
module private State =
    type State = { Events : Map<SwimEvent, int> }

    let  maxPiggyBack numMembers =
        if numMembers = 0 then 0
        else
            3. * round(log (float numMembers + 1.)) |> int

    let private sortCompare (e1, cnt1) (e2, cnt2) =
        let eventOrder = function Membership _ -> 0 | User _ -> 1
        compare (eventOrder e1) (eventOrder e2) + (cnt1 - cnt2)
        
    let add event disseminator =
        match Map.tryFind event disseminator.Events with
        | Some _ -> disseminator
        | None -> { disseminator with Events = Map.add event 0 disseminator.Events }

    let take numMembers maxSize disseminator =
        let maxPiggyBack = maxPiggyBack numMembers
        let sizeOf = Message.encodeEvent >> Message.sizeOfValues

        let rec peek acc size = function
            | (evt, inc)::tail ->
                let size' = size - (sizeOf evt)
                if size' >= 0 then peek ((evt, inc + 1) :: acc) size' tail
                else acc, tail
            | lst -> acc, lst

        let selectedEvents, restEvents = peek [] maxSize (disseminator.Events |> Map.toList 
                                                                                |> List.sortWith sortCompare)

        selectedEvents |> List.toArray
                        |> Array.collect (fst >> Message.encodeEvent),
        { Events = (selectedEvents |> List.filter (snd >> (>) maxPiggyBack)) @ restEvents |> Map.ofList }

(******* PUBLIC API *******)
    
// make : () -> Dissemination
let make () =
    let handler _ state = function
    | Event(Membership _ as membership) -> 
        State.add membership state

    | Event(User _ as user) ->
        State.add user state

    | Take(numMembers, maxSize, rChan) ->
        let events, state' = State.take numMembers maxSize state
        rChan.Reply(events)
        state'
        
    Agent.spawn { Events = Map.empty } handler
    |> Dissemination

// membership : Node -> NodeStatus -> Dissemination -> unit
let membership node status (Dissemination agent) = 
    Membership(node, status) |> Event |> Agent.post agent

// user : string -> Dissemination -> unit
let user event (Dissemination agent) =
    User event |> Event |> Agent.post agent
    
// take : int -> int -> Dissemination -> MsgPack.Values.Value[]
let take numMembers maxSize (Dissemination agent) =
    (fun rc -> Take(numMembers, maxSize, rc)) |> Agent.postAndReply agent
