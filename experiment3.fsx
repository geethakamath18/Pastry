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

let random = Random()
// let m=int(2.0**128.0)-1 //Maximum nodeID possible
let mutable r=0; // Random ID generated
let Pastry = System.create "system" <| Configuration.defaultConfig()
let system = System.create "system" <| Configuration.defaultConfig()
// let mutable nodeGenerated = new List<int>();
let mutable numNodes=0;
let mutable numRequests=0;
let mutable nodeID=0;
let mutable masterActor=new List<IActorRef>(); // Master Actor
let mutable pastryNodes=new List<IActorRef>(); 
let mutable flag=false; 
let mutable getRandom=0;
let mutable t= new List<int>();

// added by keerthi
let mutable b = 0
let swap (a: List<int>) x y =
    let tmp = a.[x]
    a.[x] <- a.[y]
    a.[y] <- tmp

type ActorMessageType = 
    |   PastryInit of int * int * int * int
    |   Receive of string
    |   InitialJoin of List<int> * int
    |   Route of string * int * int * int
    |   AddRow of int * List<int>
    |   AddLeaf of List<int>
    |   Update of int 
    |   RouteFinish of int * int * int // Master messages
    |   Master of int * int
    |   Start of string
    |   FinishedJoining
    |   SecondaryJoin
    |   StartRouting
    |   NotInBoth
    |   RouteNotInBoth

let checkPrefix(string1: string, string2: string)=
        let mutable j=0;
        while j<string1.Length && string1.Chars(j)<>string2.Chars(j) do
            j<-j+1
        j

let display = 
    printfn "yet to be completed"

// Function to convert a base 10 integer to a base 4 number and then converting it to a string
let toBase4String( num:int, length:int)=
    let mutable res="";
    let targetBase=4;
    let mutable value=num;
    res<-string "0123456789ABCDEF".[value%targetBase]+res;
    value<-targetBase;
    while value>0 do
        res<-string "0123456789ABCDEF".[value%targetBase]+res;
        value<-value/targetBase;
    let diff= length-res.Length; // Adding zeroes if the ID isn't long enough 
    if diff>0 then
        let mutable j=0;
        while j<diff do
            res<-res+string 0; // Converting 0 to string and padding the ID with it
            j<-j+1;
    res

let clone(l: List<int>)=
    let res= List<int>();
    for i in l do
        res.Add(i)
    res

// Function to get largest element in an array/list
let getMax(a:List<int>)=
    let mutable j=0;
    let mutable max=0;
    let mutable maxIndex=(-1);
    for i in a do
        if i>max then
            max<-i; // Maximum element
            maxIndex <-j; // Index of maximum element
        j <- j+1;
    max, maxIndex

// Function to get smallest element in an array/list
let getMin(a:List<int>)=
    let mutable j=0;
    let mutable min=0;
    let mutable minIndex=(-1);
    for i in a do
        if i<min then
            min<-i; // Minimum element
            minIndex <-j; // Index of minimum element
        j <- j+1;
    min, minIndex

let appendLists(l1 : List<int>,l2: List<int>) = 
    for i in l2 do
        l1.Add(i)
    l1

    // Function to create Leaf Set 
let addBuffer(all: List<int>, largerLeaf: List<int>, smallerLeaf: List<int>, myID, routingTable: List<List<int>>)=
    for i in all do
        // Adding node to larger leaf set
        if i>myID && not (largerLeaf.Contains(i)) then
            if largerLeaf.Count<4 then // If leaf set isn't full, add node to leaf set
                largerLeaf.Add(i);
            else 
                let m, mi=getMax(largerLeaf);
                if i<m then
                    largerLeaf.RemoveAt(mi);
                    largerLeaf.Add(i);
        // Adding node to smaller leaf set
        else if i<myID  && not(smallerLeaf.Contains(i)) then
            if smallerLeaf.Count < 4 then // If leaf set isn't full, add node to leaf set
                smallerLeaf.Add(i);
            else 
                let m, mi=getMin(smallerLeaf);
                if i<m then
                    smallerLeaf.RemoveAt(mi);
                    smallerLeaf.Add(i);
        
        // Checking the routing table KIRIK PART
        let samePrefix = checkPrefix(toBase4String(myID, b), toBase4String(i, b)); // Performing prefix matching

        //changed by keerthi
        let mutable x = toBase4String(i, b);
        
        //changed by keerthi
        // let mutable xtonum = int(string(x.[samePrefix]));
        let xtonum = int(string(x.[samePrefix])); // Index of string after which prefix differs 
        if int(string(routingTable.[samePrefix].[xtonum]))=(-1) then
            routingTable.[samePrefix].[xtonum]<-i   // Addinhg entries to routing table if it is empty


let pastryNode (mailbox: Actor<_>) = 

    let mutable smallerLeaf = new List<int>();
    let mutable largerLeaf = new List<int>();
    let mutable numOfBack=0;
    // let sender=mailbox.Sender();
    // let mutable numRequests=0;
    // let mutable numNodes=0;
    let mutable routingTable= new List<List<int>>();
    let mutable idSpace=0;
    let mutable myID=0;
    let mutable samePrefix =0;
    let mutable b=0;


    let rec pasteryloop() = actor{
        let! msg =  mailbox.Receive ()
        // printfn "%A" n
        let mutable dummy = 0
        // printfn "dummy"

        match msg with
        |   PastryInit(nodes, requests, identify, baseRecieved) ->
                let mutable temp=new List<int>();
                myID <- identify
                numRequests <- requests
                b <- baseRecieved;
                for i in[0..3] do
                    temp.Add(-1);
                idSpace <- int(4.00**float(b)); 
                for i in [0 .. b] do
                    routingTable.Add(temp); 
                printfn "inside pastryInit %d" myID
        |   InitialJoin(groupOne: List<int>, id) ->
                //  myID <- id
                groupOne.RemoveAt(groupOne.IndexOf(myID)); //  Removes current node's ID from group one in order to make leaf set
                // //addBuffer(groupOne);
                printfn "myid %d" myID
                
                addBuffer(groupOne, largerLeaf, smallerLeaf, myID, routingTable);
                printfn "%d" b
                for i = 0 to b-1 do
                    let mutable x = toBase4String(myID, b);
                    // // changed by keerthi
                    let mutable xthdigit = System.Char.GetNumericValue(x.[i]) |> int
                    routingTable.[i].[xthdigit] <- myID;
                // flag <- true 

        return! pasteryloop ()
    }      
    pasteryloop()


let master (mailbox: Actor<_>) = 
    let rec masterloop() = actor{
        let! msg =  mailbox.Receive ()
        b <-ceil( Math.Log(double(numNodes)) / Math.Log(double(4)))  |> int
        let nodeIDSpace = Math.Pow(float(4), float(b)) |> int 
        let mutable randomList = new List<int>()
        let mutable groupOne = new List<int>()
        let mutable groupOneSize = 0
        printfn "value of b is %d" b
        printfn "nodeIdSpace %d" nodeIDSpace
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

        for i = 0 to numNodes-1 do 
            randomList.Add(i)
        
        for i = 0 to numNodes-1 do 
            swap randomList i (random.Next(numNodes))
        
        printfn "%A and size is %d" randomList randomList.Count

        for i = 0 to numNodes - 1 do 
            groupOne.Add(randomList.[i])
        
        for i in [0 .. numNodes-1] do
            let properties = string(i)
            let actor = spawn Pastry properties pastryNode
            pastryNodes.Add(actor)
        
            // spawn the actors of type Pastry
        printfn "size of randomlist %d" randomList.Count
        printfn "size of groupOne %d" groupOne.Count
        printfn "size of pasteryNode %d" pastryNodes.Count

        match msg with
        | Start(start)-> 
            printfn "in master actor start %s" start
            for i = 0 to groupOneSize-1 do
                let cloneofgroupone = clone(groupOne)
                // printfn "random actor number %d" randomList.[i]
                pastryNodes.[randomList.[i]] <! PastryInit(numNodes, numRequests, randomList.[i], b)
                pastryNodes.[randomList.[i]]<! InitialJoin(cloneofgroupone, randomList.[i])                
                // do initial join on random list of actors
        | FinishedJoining ->
            numJoined <- numJoined + 1
            if numJoined = groupOneSize then 
                if numJoined >= numNodes then 
                    mailbox.Self <! StartRouting
                else 
                    mailbox.Self <! SecondaryJoin
            
            if numJoined > groupOneSize then 
                if numJoined = numNodes then 
                    mailbox.Self <! StartRouting
                else 
                    mailbox.Self <! SecondaryJoin
        | SecondaryJoin ->
                let mutable startID = randomList.[random.Next(numJoined)]
                pastryNodes.[startID] <! Route("Join", startID, randomList.[numJoined], -1)
                // call the actor
        | StartRouting -> 
            // broadcast message
            printfn "Routing started"
        | NotInBoth ->
            numNotInBoth <- numNotInBoth+1
        | RouteFinish (requestFrom, requestTo, hops)->
            numRouted <- numRouted + 1
            numHops <- numHops + 1
            for i = 0 to 10 do 
                if numRouted*10 = numNodes * numRequests * i then 
                    for j = 1 to i do 
                        printfn "."
                    printfn "|"
            
            if numRouted >= numNodes * numRequests then 
                printfn "\n"
                printfn "Total Routes -> %d and Total Hops %d" numRouted numHops 
                let dummy = numHops / numRouted
                printfn "Average hops per Route -> %d" dummy

        | RouteNotInBoth -> 
            numRouteNotInBoth <- numRouteNotInBoth+1

        
        return! masterloop ()
    }      
    masterloop()


let main (args:string []) =
    numNodes <-(int) args.[1] //Setting the value of number of nodes
    numRequests <- (int) args.[2]
    printfn "number of nodes %d" numNodes
    let masterActor = spawn Pastry "master" master   
    masterActor <! Start("start")
    
    while not flag do
        let mutable i = 0
        i <- i+1
    
    0
let args = fsi.CommandLineArgs 
printfn "%d" args.Length
match args.Length with //Checking number of parameters
    | 3 -> main args    
    | _ ->  main args
