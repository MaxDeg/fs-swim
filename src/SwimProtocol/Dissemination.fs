module SwimProtocol.Dissemination

[<RequireQualifiedAccess>]
type Event =
| MembershipEvent of MembershipEvent
| UserEvent of string

type private Request =
| Push of Event
| Pull of int * AsyncReplyChannel<Event[]>

    
type private PiggyBackedEvent = Event * int
type private State = PiggyBackedEvent list

let private push event state = state

let private pull numMembers state =
    let maxPiggyBack = 5. * log10 (float numMembers)
    [], state

type EventDisseminator =
    private { Agent : MailboxProcessor<Request> }
    with
        member x.Push msg =
            printfn "[Event push] %A" msg
            Push msg |> x.Agent.Post

        member x.Pull numMembers =
            (fun rc -> Pull(numMembers, rc))
            |> x.Agent.PostAndReply

        member x.Listen disseminator = ()

let create() =
    let agent = new MailboxProcessor<Request>(fun box ->
        let rec loop state = async {
            let! msg = box.Receive()
            match msg with
            | Push event -> push event state
            | Pull (numMembers, replyChan) -> 
                let membs, state' = pull numMembers state
                replyChan.Reply membs
                state'
            
            return! loop state
        }
        
        loop ())
        
    { Agent = agent }
