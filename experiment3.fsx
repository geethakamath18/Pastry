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
        let mutable j= 0;   // Changed by keerthi on 1st novemebr
        while j<string1.Length && string1.Chars(j) = string2.Chars(j) do
            j<-j+1
        j-1

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
        let mutable dd = toBase4String(myID, b)
        let mutable ib = toBase4String(i, b)
        // printfn "tobse4string of dd %A and ib is %A" dd ib
        let samePrefix = checkPrefix(toBase4String(myID, b), toBase4String(i, b)); // Performing prefix matching
        
        //changed by keerthi
        let mutable x = toBase4String(i, b);
        
        //changed by keerthi
        // let mutable xtonum = int(string(x.[samePrefix]));
        let xtonum = int(string(x.[samePrefix])); // Index of string after which prefix differs 
        // printfn "xtonum %d" xtonumz
        // printfn "same prefix for %A and %A is %A " dd ib samePrefix
         // printfn "some random %A" routingTable.[samePrefix].[xtonum]
        // printfn "before samePrefix is %d and xtoNum is %d and routingtable count is %d and cols %d" samePrefix xtonum routingTable.Count routingTable.[0].Count
        if samePrefix < routingTable.Count && int(string(routingTable.[samePrefix].[xtonum]))=(-1) then

            // printfn "after samePrefix is %d and xtoNum is %d" samePrefix xtonum
            routingTable.[samePrefix].[xtonum]<-i   // Addinhg entries to routing table if it is empty

let addNode(node: int, smallerLeaf: List<int>, largerLeaf: List<int>, myID, routingTable: List<List<int>> )=
        // Adding node to larger leaf set
        if node< myID && not(largerLeaf.Contains(node)) then
            if largerLeaf.Count<4 then
                largerLeaf.Add(node); // If leaf set isn't full, add node to leaf set
            else
                let m, mi=getMax(largerLeaf);
                if node<m then
                    largerLeaf.RemoveAt(mi);
                    largerLeaf.Add(node)
        // Adding node to smaller leaf set
        else if node< myID && not(smallerLeaf.Contains(node)) then
            if smallerLeaf.Count<4 then
                smallerLeaf.Add(node); // If leaf set isn't full, add node to leaf set
            else
                let m, mi=getMax(smallerLeaf);
                if node<m then
                    smallerLeaf.RemoveAt(mi);
                    smallerLeaf.Add(node)
            
            // Checking the routing table KIRIK PART
            let samePrefix = checkPrefix(toBase4String(myID, b), toBase4String(node, b));
            let mutable x = toBase4String(node, b);

            // added by keerthi 
            let xtoIntger = int(string(x.[samePrefix]));
            if int(string(routingTable.[samePrefix].[xtoIntger]))=(-1) then
                routingTable.[samePrefix].[xtoIntger]<- node   // changed by keerthi

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


    let rec pastryloop() = actor{
        let! msg =  mailbox.Receive ()
        let sender = mailbox.Sender();
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
                    routingTable.Add(clone(temp)); 
                // printfn "inside pastryInit %d" myID
        |   InitialJoin(groupOne: List<int>, id) ->
                //  myID <- id
                groupOne.RemoveAt(groupOne.IndexOf(myID)); //  Removes current node's ID from group one in order to make leaf set
                // //addBuffer(groupOne);
                // printfn "myid %d" myID
                // let sender=mailbox.Sender();
                addBuffer(groupOne, largerLeaf, smallerLeaf, myID, routingTable);
                // printfn "%d" b
                for i = 0 to b-1 do
                    let mutable x = toBase4String(myID, b);
                    // // changed by keerthi
                    let mutable xthdigit = System.Char.GetNumericValue(x.[i]) |> int
                    routingTable.[i].[xthdigit] <- myID;
                // flag <- true 
                sender <! FinishedJoining

        |   Route(messageRoute: string, requestFrom: int, requestTo: int, hops: int) -> // CHANGED
                match messageRoute with
                
                |   "Join" ->
                    let mutable o=0;
                    let mutable p=0;
                    let mutable diff=0;
                    let mutable nearest = -1;
                    samePrefix <- checkPrefix(toBase4String(myID, b), toBase4String(requestTo, b));
                    // printfn "in join "
                    // lets keep it common 
                    let m, mi = getMin(smallerLeaf); // Get index of smallest element in list and its index
                    let o, p = getMax(largerLeaf) // Get index of largest element in list and its index

                     // added by keerthi
                    let mutable indexToroutingTable = toBase4String(requestTo, b);
                    let intIndex = int(string(indexToroutingTable.[samePrefix]));
                    printfn "myId is %d and routingTable is %A" myID routingTable

                    if hops = -1 && samePrefix>0 then // If hops =-1, then node has just joined
                        for i in [0 .. samePrefix] do
                            t <- clone(routingTable.[i])

                            printfn "what is routing table of i %A" routingTable
                            printfn "request to is %d" requestTo
                            printfn "myId is %d and routingTable is %A" myID routingTable
                            pastryNodes.[requestTo]<! AddRow(i,t); // Adding rows to routing table to nearest node
                    pastryNodes.[requestTo] <! AddRow(samePrefix, clone(routingTable.[samePrefix]))
                    // shifting this to top
                    // changed by keerthi
                    // m, mi= getMin(smallerLeaf); // Get index of smallest element in list and its index
                    // o, p= getMax(largerLeaf) // Get index of largest element in list and its index
                    // printfn "myID %d" myID
                    if (smallerLeaf.Count>0 && requestTo>= m && requestTo<=myID) ||(largerLeaf.Count>0 && requestTo<= o && requestTo>=myID) then
                        diff <- idSpace+10;
                        printfn "in if"
                        if requestTo<myID then
                            for i in smallerLeaf do // Check if node is in smaller set and checking which node is closest
                                if abs(requestTo-i)< diff then
                                    nearest <-i;
                                    diff <- abs(requestTo-i);
                        else
                            for i in largerLeaf do // Check if node is in larger set and checking which node is closest
                                if abs(requestTo-i)< diff then
                                    nearest <-i;
                                    diff <- abs(requestTo-i);
                    
                        if abs(requestTo - myID)>diff then //If node does not exist in the leaf set
                            printfn "in abs and nearset is %d and pastry count is %d" nearest pastryNodes.Count
                            pastryNodes.[nearest]<! Route(messageRoute, requestFrom, requestTo, hops+1)
                        else
                            printfn "in else after abs and restto is %d" requestTo
                            let mutable allLeaf = new List<int>(); // Creating a leaf set
                            allLeaf.Add(myID);                    // Adding current node to leaf set
                            allLeaf <- appendLists(allLeaf,smallerLeaf);     // Adding smaller leaf set
                            allLeaf <- appendLists(allLeaf, largerLeaf);   // Adding larger leaf set
                            pastryNodes.[requestTo] <!AddLeaf(allLeaf); //Updating the leafset of the node closest to the node joining
                    
                    // changed by keerthi
                    // let o,p = getMin(smallerLeaf) // I am not sure why was this outside the indentation

                    // changed by keerthi
                    else if(smallerLeaf.Count<4 && smallerLeaf.Count>0 && requestTo<m) then // If the smallest element in the smallerLeaf set is greater than the current node ID, route message to it
                        printfn "in else if 1"
                        pastryNodes.[m]<! Route(messageRoute, requestFrom, requestTo, hops+1)
                    
                    // changed by keerthi 
                    //o,p <- getMax(largerLeaf)  // I am not sure why was this outside the indentation

                    else if(largerLeaf.Count<4 && largerLeaf.Count>0 && requestTo> o) then // If the largest element in the largerLeaf set is smaller than the current node ID, route message to it
                        printfn "in else if 2"
                        pastryNodes.[o]<! Route(messageRoute, requestFrom, requestTo, hops+1)
                    
                    else if ((smallerLeaf.Count = 0 && requestTo<myID) || (largerLeaf.Count = 0 && requestTo>myID) ) then
                        printfn "in else if 3"
                        let mutable allLeaf = new List<int>();
                        allLeaf.Add(myID);
                        allLeaf <- appendLists(allLeaf,smallerLeaf);
                        allLeaf <- appendLists(allLeaf, largerLeaf);
                        pastryNodes.[requestTo] <!AddLeaf(allLeaf);

                        // why these two lines are outside elseif??  changed by keerthi
                    //let mutable x = toBase4String(requestTo, b);
                    //x <- int(string(x.[samePrefix]));

                     // changed by keerthi
                    else if routingTable.[samePrefix].[intIndex] <> -1 then
                        // printfn "in else if 4"
                        pastryNodes.[routingTable.[samePrefix].[intIndex]]<!Route(messageRoute, requestFrom, requestTo, hops+1)

                    else if requestTo>myID then
                        printfn "in else if 5"
                        // changed by keerthi
                        let m,mi = getMax(largerLeaf)
                        pastryNodes.[m]<! Route(messageRoute,requestFrom, requestTo, hops+1)
                        masterActor.[0] <! NotInBoth

                    else if requestTo<myID then
                        printfn "in else if 6"
                        // changed by keerthi
                        let o,p = getMin(smallerLeaf)
                        pastryNodes.[o]<! Route(messageRoute,requestFrom, requestTo, hops+1)
                        masterActor.[0] <! NotInBoth
                    
                    else
                        printfn("Not Possible")

                |   "Route" ->
                    let mutable o=0;
                    let mutable p=0;
                    let mutable diff=0;
                    let mutable nearest= -1;
                    // printfn "in route "
                    if myID=requestTo then
                        masterActor.[0] <! RouteFinish(requestFrom,requestTo,hops+1)
                        // printfn "in Route"
                    else 
                        samePrefix <- checkPrefix(toBase4String(myID, b), toBase4String(requestTo, b));
                        // printfn "samePrefix "
                        let m, mi= getMin(smallerLeaf); // Get index of smallest element in list and its index
                        let o, p= getMax(largerLeaf) // Get index of largest element in list and its index

                        let mutable x = toBase4String(requestTo, b);
                        let xtoIntobasefour = int(string(x.[samePrefix]));

                        if (smallerLeaf.Count>0 && requestTo>= m && requestTo<=myID) ||(largerLeaf.Count>0 && requestTo<= o && requestTo>=myID) then
                            diff <- idSpace+10;
                            // printfn "in if of else part and diff is %d" diff
                            if requestTo<myID then
                                for i in smallerLeaf do // Check if node is in smaller set and checking which node is closest
                                    if abs(requestTo-i)< diff then
                                        nearest <-i;
                                        diff <- abs(requestTo-i);
                            else
                                for i in largerLeaf do // Check if node is in larger set and checking which node is closest
                                    if abs(requestTo-i)< diff then
                                        nearest <-i;
                                        diff <- abs(requestTo-i);
                        
                            if abs(requestTo - myID)>diff then //If node does not exist in the leaf set
                                pastryNodes.[nearest]<! Route(messageRoute, requestFrom, requestTo, hops+1)
                            else
                               masterActor.[0] <! RouteFinish(requestFrom, requestTo, hops+1)

                        // Doubt   Changed by keerthi
                        // o,p <- getMin(smallerLeaf)  // same thing

                        // changed by keerthi
                        else if(smallerLeaf.Count<4 && smallerLeaf.Count>0 && requestTo<m) then // If the smallest element in the smallerLeaf set is greater than the current node ID, route message to it
                            printfn "inelse if 1"
                            pastryNodes.[m]<! Route(messageRoute, requestFrom, requestTo, hops+1)
                        
                        // // // here again  changed by keerthi 
                        // // // o,p <- getMax(largerLeaf)   
                        else if(largerLeaf.Count<4 && largerLeaf.Count>0 && requestTo> o) then // If the largest element in the largerLeaf set is smaller than the current node ID, route message to it
                            printfn "inelse if 2"
                            pastryNodes.[o]<! Route(messageRoute, requestFrom, requestTo, hops+1)
                        
                        else if ((smallerLeaf.Count = 0 && requestTo<myID) || (largerLeaf.Count = 0 && requestTo>myID) ) then
                            printfn "inelse if 3"
                            masterActor.[0] <! RouteFinish(requestFrom, requestTo, hops+1)
                        
                        // // // changed by keerthi 
                        // // // let mutable x = toBase4String(requestTo, b);
                        // // // x <- int(string(x.[samePrefix]));
                        else if routingTable.[samePrefix].[xtoIntobasefour] <> -1 then
                            // printfn "inelse if 4"
                            pastryNodes.[routingTable.[samePrefix].[xtoIntobasefour]] <! Route(messageRoute, requestFrom, requestTo, hops+1)

                        else if requestTo>myID then
                            printfn "inelse if 5"
                            let m,mi = getMax(largerLeaf)
                            pastryNodes.[m]<! Route(messageRoute,requestFrom, requestTo, hops+1)
                            masterActor.[0] <! NotInBoth

                        else if requestTo<myID then
                            let o,p = getMin(smallerLeaf)
                            pastryNodes.[o]<! Route(messageRoute,requestFrom, requestTo, hops+1)
                            masterActor.[0]<! NotInBoth
                        
                        else
                            printfn("Not Possible")
                | _ -> printfn "Message not recognised"
        |   Receive(messageReceived) ->
                match messageReceived with
                |   "StartRouting" ->
                    // printfn "in start routing of pastry node"
                    // printfn " numrequests is %d" numRequests
                    for i in [1 .. numRequests] do
                        // printfn "before thread sleep"
                        // System.Threading.Thread.Sleep(1000);    do not forget to uncomment
                        getRandom <- random.Next(numNodes);
                        pastryNodes.[getRandom] <! Route ("Route", myID, getRandom, int(-1)); //szxdfcgvhjnkml,sdfgvxdfghhgfdgfdgfdsfgbnv gfdghjm
                        
        |   AddLeaf(allLeaf) ->
                printfn "in Add leaf"
                addBuffer(allLeaf, smallerLeaf, largerLeaf, myID, routingTable);
                printfn "after add buffer"
                for i in smallerLeaf do
                    numOfBack <- numOfBack+1;
                    pastryNodes.[i]<! Update(myID);
                printfn "after first for loop"
                for i in largerLeaf do
                    numOfBack <- numOfBack+1;
                    pastryNodes.[i] <! Update(myID);
                printfn "after second for loop"
                for i in[0 .. b] do
                    for j in [0 .. 3] do
                        if routingTable.[i].[j] <> -1 then
                            numOfBack <- numOfBack+1
                            pastryNodes.[routingTable.[i].[j]] <!Update(myID)
                printfn "after third for loop"
                for i in [0 .. b] do
                    let mutable x = toBase4String(myID, b);
                    let xtoInte =  int(x.[i]);
                    printfn "xtointe is %d and i is %d" xtoInte i
                    printfn "routingtable row is %d and routingtable col is %d" routingTable.Count routingTable.[0].Count
                    routingTable.[i].[xtoInte] <- myID
                printfn "after fourth for loop"

        |   AddRow(rowNum, newRow) ->
            for i in [0 .. 3] do
                // printfn "rowNum is %d and the row is is %d" rowNum routingTable.Count
                if routingTable.[rowNum].[i] = -1 then
                    routingTable.[rowNum].[i] <- newRow.[i];
        |   Update(newNodeID) ->
                addNode(newNodeID, smallerLeaf, largerLeaf, myID, routingTable);
                sender<!"Acknowledgement"

        | _ -> printfn "Message not recognised in Recive %A" msg
                
        return! pastryloop ()
    }      
    pastryloop()


let master (mailbox: Actor<_>) = 

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

    for i = 0 to numNodes - 1 do 
        pastryNodes.[randomList.[i]] <! PastryInit(numNodes, numRequests, randomList.[i], b)
        // spawn the actors of type Pastry
    printfn " routing table of some node is %A" pastryNodes.[0]
    printfn "size of randomlist %d" randomList.Count
    printfn "size of groupOne %d" groupOne.Count
    printfn "size of pasteryNode %d" pastryNodes.Count
    let rec masterloop() = actor{
        let! msg =  mailbox.Receive ()
        match msg with
        | Start(start)-> 
            printfn "in master actor start %s" start
            for i = 0 to groupOneSize-1 do
                let cloneofgroupone = clone(groupOne)
                // printfn "random actor number %d" randomList.[i]

                pastryNodes.[randomList.[i]]<! InitialJoin(cloneofgroupone, randomList.[i]) 
                
                // do initial join on random list of actors
        | FinishedJoining ->
            numJoined <- numJoined + 1
            // printfn "in finished join"
            // printfn "total joined nodes is %d" numJoined
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
                printfn "startID is %A" startID
                pastryNodes.[startID] <! Route("Join", startID, randomList.[numJoined], -1)
                // call the actor
        | StartRouting -> 
            // broadcast message
            for i = 0 to groupOneSize - 1 do 
                pastryNodes.[i] <! Receive("StartRouting") 
            printfn "Routing started"
        | NotInBoth ->
            numNotInBoth <- numNotInBoth+1
        | RouteFinish (requestFrom, requestTo, hops)->
            numRouted <- numRouted + 1
            numHops <- numHops + hops
            for i = 0 to 10 do 
                if numRouted*10 = numNodes * numRequests * i then 
                    for j = 1 to i do 
                        printf "."
                    printf "|"
            // printfn "numRouted is %d and numNodes is %d and numRequests is %d" numRouted numNodes numRequests
            if numRouted >= numNodes * (numRequests - 10) then 
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
    let masterActorNode = spawn Pastry "master" master 
    masterActor.Add(masterActorNode)  
    masterActor.[0] <! Start("start")
    
    while not flag do
        let mutable i = 0
        i <- i+1
    
    0
let args = fsi.CommandLineArgs 
printfn "%d" args.Length
match args.Length with //Checking number of parameters
    | 3 -> main args    
    | _ ->  main args
