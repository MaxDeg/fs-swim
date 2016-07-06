namespace SwimProtocol

type Disseminator = private { Events : Map<SwimEvent, int> }

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Disseminator =
    let private add event disseminator =
        match Map.tryFind event disseminator.Events with
        | Some _ -> disseminator
        | None -> { disseminator with Events = Map.add event 0 disseminator.Events }
        
    let private maxPiggyBack numMembers =
        if numMembers = 0 then 0
        else
            3. * round(log (float numMembers + 1.)) |> int

    let private sortCompare (e1, cnt1) (e2, cnt2) =
        let eventOrder = function Membership _ -> 0 | User _ -> 1
        compare (eventOrder e1) (eventOrder e2) + (cnt1 - cnt2)

    // make : () -> Disseminator
    let make () = { Events = Map.empty }

    // membership : Node -> NodeStatus -> Disseminator -> Disseminator
    let membership node status = Membership(node, status) |> add

    // user : string -> Disseminator -> Disseminator
    let user event = User event |> add

    // take : int -> Disseminator
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
