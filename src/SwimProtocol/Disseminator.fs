namespace SwimProtocol

type Disseminator = private { Events : Map<SwimEvent, uint32> }

[<RequireQualifiedAccess; CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Disseminator =
    let private add event disseminator =
        match Map.tryFind event disseminator.Events with
        | Some _ -> disseminator
        | None -> { disseminator with Events = Map.add event 0u disseminator.Events }
        
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

    // take : int -> int -> Disseminator
    let take numMembers maxSize disseminator =
        let maxPiggyBack = maxPiggyBack numMembers

        [], disseminator
