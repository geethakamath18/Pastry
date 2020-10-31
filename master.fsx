#time "on"
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Akka.TestKit
open System.Collections.Generic
open Akka.FSharp
open System.Diagnostics
let mutable numNodes = 0
let mutable numRequests = 0
let system = System.create "system" <| Configuration.defaultConfig()
let random = System.Random()

let mutable flag = false
type ActorMessageType = 
    | Master of int * int
    | RouteFinish of int * int * int
// base == b 
let swap (a: List<int>) x y =
    let tmp = a.[x]
    a.[x] <- a.[y]
    a.[y] <- tmp



let master (mailbox: Actor<_>) = 
    let rec masterloop() = actor{
        let! msg =  mailbox.Receive ()
        let b = ceil( Math.Log(double(numNodes)) / Math.Log(double(4)))  |> int
        let nodeIDSpace = Math.Pow(float(4), float(b)) |> int 
        let mutable randomList = new List<int>()
        let mutable groupOne = new List<int>()
        let mutable groupOneSize = 0

        printfn "%d" nodeIDSpace
        if numNodes <= 1024 then 
            groupOneSize <- numNodes
        else 
            groupOneSize <- 1024
        
        let mutable i = -1
        let mutable numHops = 0
        let mutable numJoined = 0
        let mutable numNotInBoth = 0
        let mutable numRouteNotInBoth = 0
        let mutable numRouted = 0

        let mutable rnd = random.Next(nodeIDSpace)
        for i = 0 to nodeIDSpace-1 do 
            randomList.Add(i)
        
        for i = 0 to nodeIDSpace-1 do 
            swap randomList i (random.Next(nodeIDSpace))
        
        printfn "%A and size is %d" randomList randomList.Count

        for i = 0 to groupOneSize - 1 do 
            groupOne.Add(randomList.[i])
        
        for i = 0 to numNodes do

            // spawn the actors of type Pastry

        match msg with
        | "Start" -> 
            for i = 0 to groupOneSize
                // do initial join on random list of actors
        | "FinishedJoining" ->
            numJoined += 1
            if numJoined = groupOneSize then 
                if numJoined >= numNodes then 
                    self <| StartRouting
                else 
                    self <| SecondaryJoin
            
            if numJoined > groupOneSize then 
                if numJoined == numNodes then 
                    self <| StartRouting
                else 
                    self <| SecondaryJoin
        | "SecondaryJoin" ->
                let mutable startID = randomList(random.next(numJoined))
                // call the actor
        | "StartRouting" -> 
            // broadcast message
            printfn "Routing started"
        | "NotInBoth" ->
            numNotInBoth += 1
        | RouteFinish (requestFrom, requestTo, hops)->
            numRouted <- numRouted + 1
            numHops <- numHops + 1
            for i = 0 to 10 do 
                if numRouted*10 == numNodes * numRequests * i then 
                    for j = 1 to i do 
                        printfn "."
                    printfn "|"
            
            if numRouted >= numNodes * numRequests then 
                printfn "\n"
                prinfn "Total Routes -> %d and Total Hops %d" numRouted numHops 
                prinfn "Average hops per Route -> %A" numHops / numRouted
        | "RouteNotInBoth" -> 
            numRouteNotInBoth <- numRouteNotInBoth+1

        flag <- true
        return! masterloop ()
    }      
    masterloop()


let main (args:string []) =
    numNodes <-(int) args.[1] //Setting the value of number of nodes
    numRequests <- (int) args.[2]

    let masterActor = spawn system "master" master   
    masterActor <! Master(numNodes, numRequests)
    
    while not flag do
        let mutable i = 0
        i <- i+1
    
    0
let args = fsi.CommandLineArgs 
printfn "%d" args.Length
match args.Length with //Checking number of parameters
    | 3 -> main args    
    | _ ->  main args  
