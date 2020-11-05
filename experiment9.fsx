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

let mutable indexToroutingTable = "";
let mutable rowRouting = 0;

let Pastry = System.create "system" <| Configuration.defaultConfig()
let mutable numNodes=0;
let mutable numRequests=0;
let mutable masterActor=new List<IActorRef>(); 
let mutable flag = false;
let mutable base_b = 0;
let random = Random();
let mutable mapofinitActors = Map.empty<string, IActorRef>
let mutable getRandom=0;

type ActorMessageType = 
    |   PastryInit of int * int * int // * int
    |   Receive of string
    |   InitialJoin of List<int>
    |   Route of string * int * int * int
    |   AddRow of int * List<int>
    |   AddLeaf of List<int>
    |   Update of int 
    |   RouteFinish of int * int * int // Master messages
    // |   Master of int * int
    |   Start
    |   FinishedJoining
    |   SecondaryJoin
    |   StartRouting
    |   NotInBoth
    // |   RouteNotInBoth
    |   Init

let swap (a: List<int>) x y =
    let tmp = a.[x]
    a.[x] <- a.[y]
    a.[y] <- tmp

let appendLists(l1 : List<int>,l2: List<int>) = 
    for i in l2 do
        l1.Add(i)
    l1

let clone(l: List<int>)=
    let res= List<int>();
    for i in l do
        res.Add(i)
    res

// Function to get largest element in an array/list
let getMax(a:List<int>)=
    let mutable j=0;
    let mutable max=System.Int32.MinValue;
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
    let mutable min=System.Int32.MaxValue;
    let mutable minIndex=(-1);
    for i in a do
        if i<min then
            min<-i; // Minimum element
            minIndex <-j; // Index of minimum element
        j <- j+1;
    min, minIndex

let toBase4String( num:int, length:int)=
    let mutable res="";
    let targetBase=4;
    let mutable value=num;
    // res<-string "0123456789ABCDEF".[value%targetBase]+res;
    // value<-num;
    while value>0 do
        res<-string "0123456789ABCDEF".[value%targetBase]+res;
        value<-value/targetBase;
    let diff= length-res.Length; // Adding zeroes if the ID isn't long enough 
    if diff>0 then
        let mutable j=0;
        while j<diff do
            res<-string 0+res; // Converting 0 to string and padding the ID with it
            j<-j+1;
    res

let checkPrefix(string1: string, string2: string)=
    let mutable j= 0;
    let mutable prefixMatch = 0  // Changed by keerthi on 1st novemebr
    while j<string1.Length && string1.Chars(j) = string2.Chars(j) do
        j<-j+1
    if j > 0 then   
        j <- j - 1
    j 


let pastryNode (mailbox: Actor<_>) = 
    let mutable smallerLeaf = new List<int>();
    let mutable largerLeaf = new List<int>();
    let mutable numOfBack=0;
    let mutable routingTable= new List<List<int>>();
    let mutable idSpace=0;
    let mutable myID=0;
    let mutable samePrefix =0;

    let addNode(node: int)=
        // Adding node to larger leaf set
        if node> myID && not(largerLeaf.Contains(node)) then
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
                let m, mi=getMin(smallerLeaf);
                if node>m then
                    smallerLeaf.RemoveAt(mi);
                    smallerLeaf.Add(node)
            
            // Checking the routing table KIRIK PART
        let samePrefix = checkPrefix(toBase4String(myID, base_b), toBase4String(node, base_b));
        let mutable x = toBase4String(node, base_b);

        // added by keerthi 
        let xtoIntger = int(string(x.[samePrefix]));
        if int(string(routingTable.[samePrefix].[xtoIntger]))=(-1) then
            routingTable.[samePrefix].[xtoIntger]<- node

    let addBuffer(all: List<int>)=
        // printfn "Enetered add buffer"
        for i in all do
            // printfn "Enetered for"
            if i>myID && not (largerLeaf.Contains(i)) then
                // printfn "Enetered if"
                if largerLeaf.Count<4 then
                    // printfn "Enetered 1" // If leaf set isn't full, add node to leaf set
                    largerLeaf.Add(i);
                else
                    // printfn "Enetered 2" 
                    let max, maxIndex=getMax(largerLeaf);
                    if i<max then
                        // printfn "Enetered 3"
                        largerLeaf.RemoveAt(maxIndex);
                        largerLeaf.Add(i);
            // Adding node to smaller leaf set
            else if i<myID  && not(smallerLeaf.Contains(i)) then
                // printfn "Enetered else"
                if smallerLeaf.Count < 4 then 
                    // printfn "Enetered 4"// If leaf set isn't full, add node to leaf set
                    smallerLeaf.Add(i);
                else 
                    // printfn "Enetered 5"
                    let min, minIndex=getMin(smallerLeaf);
                    if i<min then
                        // printfn "Enetered 6"
                        smallerLeaf.RemoveAt(minIndex);
                        smallerLeaf.Add(i);
            // printfn "above same prefix"
            let samePrefix = checkPrefix(toBase4String(myID, base_b), toBase4String(i, base_b)); // Performing prefix matching
            // printfn "after same prefix and sameprefix is %d" samePrefix
            let mutable x = toBase4String(i, base_b);
            // printfn "the value of x is %A" x
            let xtonum = int(string(x.[samePrefix])); 
            // printfn "SAME PREFIX IS %d and xtoNUM is %d" samePrefix xtonum// Index of string after which prefix differs 
            if int(string(routingTable.[samePrefix].[xtonum]))=(-1) then
                routingTable.[samePrefix].[xtonum]<-i   // Addinhg entries to routing table if it is empty
    
    let rec pastryloop() = actor{
        let! msg= mailbox.Receive();
        let sender = mailbox.Sender();
        match msg with
        |   PastryInit(nodes, requests, identify) -> //, baseRecieved) ->
                let mutable temp=new List<int>();
                myID <- identify
                numRequests <- requests
                for i in[0..3] do
                    temp.Add(-1);
                idSpace <- int(4.00**float(base_b)); 
                for i in [0 .. base_b-1] do
                    routingTable.Add(clone(temp));
                //printfn "my ID is %d and routing table is %A and base_b is %d and IDSPACE is %d" myID routingTable base_b idSpace
        |   InitialJoin(groupOne: List<int>) ->
                groupOne.RemoveAt(groupOne.IndexOf(myID)); //  Removes current node's ID from group one in order to make leaf set
                addBuffer(groupOne);
                for i = 0 to (base_b-1) do
                    let mutable x = toBase4String(myID, base_b);
                    let mutable xthdigit = System.Char.GetNumericValue(x.[i]) |> int
                    routingTable.[i].[xthdigit] <- myID;
                // flag <- true 
                // printfn "routing table is %A and myID is %d" routingTable myID
                sender <! FinishedJoining
        |   Route(messageRoute: string, requestFrom: int, requestTo: int, hops: int) ->
                match messageRoute with 
                |   "Join" -> 
                    let mutable o=0;
                    let mutable p=0;
                    let mutable diff=0;

                    samePrefix <- checkPrefix(toBase4String(myID, base_b), toBase4String(requestTo, base_b));
                    indexToroutingTable <-toBase4String(requestTo, base_b);
                    rowRouting <- int(string(indexToroutingTable.[samePrefix]));
                    if hops = -1 && samePrefix>0 then // If hops =-1, then node has just joined
                        for i in [0 .. samePrefix-1] do
                            // t <- clone(routingTable.[i])

                            printfn "what is routing table of i %A" routingTable
                            printfn "request to is %d" requestTo
                            printfn "myId is %d and routingTable is %A" myID routingTable
                            mapofinitActors.[string(requestTo)] <! AddRow(i,clone(routingTable.[i]))
                            // pastryNodes.[requestTo]<! AddRow(i,t); 
                    mapofinitActors.[string(requestTo)] <! AddRow(samePrefix,clone(routingTable.[samePrefix]))       
                    let minimum, minIndex = getMin(smallerLeaf); // Get index of smallest element in list and its index
                    let maximum, maxIndex = getMax(largerLeaf) // Get index of largest element in list and its index

                    // let mutable indexToroutingTable = toBase4String(requestTo, base_b);
                    // let rowRouting = int(string(indexToroutingTable.[samePrefix]));

                    if (smallerLeaf.Count>0 && requestTo>= minimum && requestTo<=myID) ||(largerLeaf.Count>0 && requestTo<= maximum && requestTo>=myID) then
                        diff <- idSpace+10;
                        // printfn "in if"
                        let mutable nearest = -1;
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
                            // printfn "in abs and nearset is %d and pastry count is %d" nearest pastryNodes.Count
                            // pastryNodes.[nearest]<! Route(messageRoute, requestFrom, requestTo, hops+1)
                            mapofinitActors.[string(nearest)] <! Route(messageRoute, requestFrom, requestTo, hops+1)
                        else
                            // printfn "in else after abs and restto is %d" requestTo
                            let mutable allLeaf = new List<int>(); // Creating a leaf set
                            allLeaf.Add(myID);                    // Adding current node to leaf set
                            allLeaf <- appendLists(allLeaf,smallerLeaf);     // Adding smaller leaf set
                            allLeaf <- appendLists(allLeaf, largerLeaf);   // Adding larger leaf set
                            mapofinitActors.[string(requestTo)] <!AddLeaf(allLeaf); //Updating the leafset of the node closest to the node joining

                    else if(smallerLeaf.Count<4 && smallerLeaf.Count>0 && requestTo<minimum) then // If the smallest element in the smallerLeaf set is greater than the current node ID, route message to it
                        // printfn "in else if 1"
                        mapofinitActors.[string(minimum)] <! Route(messageRoute, requestFrom, requestTo, hops+1)

                    else if(largerLeaf.Count<4 && largerLeaf.Count>0 && requestTo> maximum) then // If the largest element in the largerLeaf set is smaller than the current node ID, route message to it
                        // printfn "in else if 2"
                        mapofinitActors.[string(maximum)] <! Route(messageRoute, requestFrom, requestTo, hops+1) 

                    else if ((smallerLeaf.Count = 0 && requestTo<myID) || (largerLeaf.Count = 0 && requestTo>myID) ) then
                        // printfn "in else if 3"
                        let mutable allLeaf = new List<int>();
                        allLeaf.Add(myID);
                        allLeaf <- appendLists(allLeaf,smallerLeaf);
                        allLeaf <- appendLists(allLeaf, largerLeaf);
                        mapofinitActors.[string(requestTo)] <!AddLeaf(allLeaf);      
                    
                    else if routingTable.[samePrefix].[rowRouting] <> -1 then
                        // printfn "in else if 4"
                        mapofinitActors.[string(routingTable.[samePrefix].[rowRouting])]<!Route(messageRoute, requestFrom, requestTo, hops+1)


                    else if requestTo>myID then
                        // printfn "in else if 5"
                        // changed by keerthi
                        let m,mi = getMax(largerLeaf)
                        mapofinitActors.[string(m)]<! Route(messageRoute,requestFrom, requestTo, hops+1)
                        masterActor.[0] <! NotInBoth

                    else if requestTo<myID then
                        // printfn "in else if 6"
                        // changed by keerthi
                        let o,p = getMin(smallerLeaf)
                        mapofinitActors.[string(o)]<! Route(messageRoute,requestFrom, requestTo, hops+1)
                        masterActor.[0] <! NotInBoth

                    
                    
                    else
                        printfn("Not Possible")
                |   "Route" ->
                    // let mutable diff=0;
                    // printfn "in route "
                    if myID=requestTo then
                        masterActor.[0] <! RouteFinish(requestFrom,requestTo,hops+1)
                    else 
                        samePrefix <- checkPrefix(toBase4String(myID, base_b), toBase4String(requestTo, base_b));
                        // printfn "samePrefix "
                        let minimum, minIndex = getMin(smallerLeaf); // Get index of smallest element in list and its index
                        let maximum, maxIndex= getMax(largerLeaf) // Get index of largest element in list and its index

                        let mutable x = toBase4String(requestTo, base_b);
                        let xtoRow = int(string(x.[samePrefix])); 

                        if (smallerLeaf.Count>0 && requestTo>= minimum && requestTo<=myID) ||(largerLeaf.Count>0 && requestTo<= maximum && requestTo>=myID) then
                            let mutable  diff = idSpace+10;
                            // printfn "in if of else part and diff is %d" diff
                            let mutable nearest= -1;
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
                                mapofinitActors.[string(nearest)]<! Route(messageRoute, requestFrom, requestTo, hops+1)
                            else
                               masterActor.[0] <! RouteFinish(requestFrom, requestTo, hops+1)
                            
                        else if(smallerLeaf.Count<4 && smallerLeaf.Count>0 && requestTo<minimum) then // If the smallest element in the smallerLeaf set is greater than the current node ID, route message to it
                            // printfn "inelse if 1"
                            mapofinitActors.[string(minimum)]<! Route(messageRoute, requestFrom, requestTo, hops+1)

                        else if(largerLeaf.Count<4 && largerLeaf.Count>0 && requestTo> maximum) then // If the largest element in the largerLeaf set is smaller than the current node ID, route message to it
                            // printfn "inelse if 2"
                            mapofinitActors.[string(maximum)]<! Route(messageRoute, requestFrom, requestTo, hops+1) 

                        else if ((smallerLeaf.Count = 0 && requestTo<myID) || (largerLeaf.Count = 0 && requestTo>myID) ) then
                            //  printfn "inelse if 3"
                            masterActor.[0] <! RouteFinish(requestFrom, requestTo, hops+1)  

                        else if routingTable.[samePrefix].[xtoRow] <> -1 then
                            // printfn "inelse if 4"
                            mapofinitActors.[string(routingTable.[samePrefix].[xtoRow])] <! Route(messageRoute, requestFrom, requestTo, hops+1)

                        else if requestTo>myID then
                            // printfn "inelse if 5"
                            let m,mi = getMax(largerLeaf)
                            mapofinitActors.[string(m)]<! Route(messageRoute,requestFrom, requestTo, hops+1)
                            masterActor.[0] <! NotInBoth

                        else if requestTo<myID then
                            let o,p = getMin(smallerLeaf)
                            mapofinitActors.[string(o)]<! Route(messageRoute,requestFrom, requestTo, hops+1)
                            masterActor.[0]<! NotInBoth
                        
                        else
                            printfn("Not Possible")
                |_ -> printfn "WRONG MESSAGE"
        
        |   AddRow(rowNum, newRow) ->
                for i in [0 .. 3] do
                    //  printfn "rowNum is %d and the row is is %d" rowNum routingTable.Count
                    if routingTable.[rowNum].[i] = -1 then
                        routingTable.[rowNum].[i] <- newRow.[i]
        |   AddLeaf(allLeaf) ->
                //printfn "in Add leaf"
                addBuffer(allLeaf);
                //printfn "after add buffer"
                for i in smallerLeaf do
                    numOfBack <- numOfBack+1;
                    mapofinitActors.[string(i)]<! Update(myID);
                //printfn "after first for loop"
                for i in largerLeaf do
                    numOfBack <- numOfBack+1;
                    mapofinitActors.[string(i)] <! Update(myID);
                //printfn "after second for loop"
                for i in[0 .. base_b-1] do
                    for j in [0 .. 3] do
                        if routingTable.[i].[j] <> -1 then
                            numOfBack <- numOfBack+1
                            mapofinitActors.[string(routingTable.[i].[j])]<!Update(myID)
                //printfn "after third for loop"
                for i in [0 .. base_b-1] do
                    let mutable x = toBase4String(myID, base_b);
                    let xtoInte =  int(string(x.[i]));
                    // printfn "xtointe is %d and i is %d" xtoInte i
                    // printfn "routingtable row is %d and routingtable col is %d" routingTable.Count routingTable.[0].Count
                    routingTable.[i].[xtoInte] <- myID
                // printfn "after fourth for loop"
        |   Receive(messageReceived) ->
            match messageReceived with
            |   "StartRouting" ->
                let totalNumReuests = numRequests + int(float(numNodes) * float(0.2))
                for i in [0 .. totalNumReuests+2] do
                    // printfn "before thread sleep"
                    System.Threading.Thread.Sleep(1000);    // do not forget to uncomment
                    getRandom <- random.Next(idSpace);
                    mailbox.Self <! Route ("Route", myID, getRandom, -1);
            |_ -> printfn "WRONG MESSAGE"   
        |   Update(newNodeID) ->
                addNode(newNodeID);
                sender<!"Acknowledgement"
        
        | _ -> printfn "WRONG MESSAGE"
        return! pastryloop ()
    }      
    pastryloop()

let master (mailbox: Actor<_>) = 
    base_b <-ceil( Math.Log(double(numNodes)) / Math.Log(double(4)))  |> int
    let nodeIDSpace = Math.Pow(float(4), float(base_b)) |> int 
    let mutable randomList = new List<int>()
    let mutable groupOne = new List<int>()
    let mutable groupOneSize = 0
    let mutable i = -1
    let mutable numHops = 0
    let mutable numJoined = 0
    let mutable numNotInBoth = 0
    let mutable numRouteNotInBoth = 0
    let mutable numRouted = 0

    if numNodes <= 100 then 
        groupOneSize <- numNodes
    else 
        groupOneSize <- 100
    // groupOneSize <-numNodes

    let rec masterloop() = actor{
        let! msg =  mailbox.Receive ()
        match msg with
        | Init -> 
            printfn "Initiliazing Nodes"
            for i = 0 to nodeIDSpace-1 do 
                randomList.Add(i)

            for i = 0 to nodeIDSpace-1 do 
                swap randomList i (random.Next(randomList.Count - 1))

            for i = 0 to groupOneSize - 1 do 
                groupOne.Add(randomList.[i])
            
            for i in [0 .. numNodes-1] do
                let properties = string(randomList.[i])
                let actor = spawn Pastry properties pastryNode
                actor <! PastryInit(numNodes, numRequests, randomList.[i])// base_b)
                mapofinitActors <- mapofinitActors.Add(properties, actor)
            // printfn "Initilization Complete"
            System.Threading.Thread.Sleep(10) // Implement using flag
            mailbox.Self <! Start
        |   Start-> 
            // printfn "in master actor staRT" 
            for i = 0 to groupOneSize-1 do
                mapofinitActors.[string(randomList.[i])] <! InitialJoin(clone(groupOne))
        |   FinishedJoining ->
            numJoined <- numJoined + 1
            //printfn "in finished join"
            // if numJoined % int(numNodes / 1) = 0 then 
            //     printfn "Total nodes joined is %d" numJoined
            if numJoined > groupOneSize then 
                mailbox.Self <! SecondaryJoin
            if numJoined >= groupOneSize then 
                mailbox.Self <! StartRouting
            
            // if numJoined = groupOneSize then 
            //     if numJoined >= numNodes then 
            //         mailbox.Self <! StartRouting
            //     else 
            //         mailbox.Self <! SecondaryJoin
            
            // if numJoined > groupOneSize then 
            //     if numJoined = numNodes then 
            //         mailbox.Self <! StartRouting
            //     else 
            //         mailbox.Self <! SecondaryJoin
        | SecondaryJoin ->
                let mutable startID = randomList.[random.Next(numJoined)]
                //printfn "startID is %A" startID
                mapofinitActors.[string(startID)] <! Route("Join", startID, randomList.[numJoined], -1)
                // call the actor
        | StartRouting -> 
            // broadcast message
            for i = 0 to numNodes - 1 do 
                mapofinitActors.[string(randomList.[i])] <! Receive("StartRouting") 
            //printfn "Routing started"
        | RouteFinish (requestFrom, requestTo, hops)->
            //hops <- hops+1 
            numRouted <- numRouted + 1
            numHops <- numHops + hops + 1
            // printfn "numRouted is %d and numNodes is %d and numRequests is %d" numRouted numNodes numRequests
            // printfn "numRouted %d" numRouted 
            if numRouted = numNodes * (numRequests) then 
                printfn "\n"
                printfn "Total Routes -> %d and Total Hops %d" numRouted numHops 
                let dummy = float(numHops) / float(numRouted)
                printfn "Average hops per Route in float-> %f" dummy
                // printfn "Average hops per Route in float-> %f" (ceil(dummy))
                flag <- true
        | NotInBoth ->
            numNotInBoth <- numNotInBoth+1
        | _ -> printfn "WRONG MESSAGE"
        return! masterloop ()
    }      
    masterloop()

let main (args:string []) =
    numNodes <-(int) args.[1] //Setting the value of number of nodes
    numRequests <- (int) args.[2]
    // printfn "number of nodes %d" numNodes
    let masterActorNode = spawn Pastry "master" master 
    masterActor.Add(masterActorNode)  
    masterActor.[0] <! Init
    
    while not flag do
        let mutable i = 0
        i <- i+1
    
    0
let args = fsi.CommandLineArgs 
// printfn "%d" args.Length
match args.Length with //Checking number of parameters
    | 3 -> main args    
    | _ ->  main args
