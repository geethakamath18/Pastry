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
            
        // Checking the routing table
        let samePrefix = checkPrefix(toBase4String(myID, base_b), toBase4String(node, base_b));
        let mutable x = toBase4String(node, base_b);
        let xtoIntger = int(string(x.[samePrefix]));
        if int(string(routingTable.[samePrefix].[xtoIntger]))=(-1) then
            routingTable.[samePrefix].[xtoIntger]<- node

    let addBuffer(all: List<int>)=
        for i in all do
            if i>myID && not (largerLeaf.Contains(i)) then
                if largerLeaf.Count<4 then
                    largerLeaf.Add(i);
                else
                    let max, maxIndex=getMax(largerLeaf);
                    if i<max then
                        largerLeaf.RemoveAt(maxIndex);
                        largerLeaf.Add(i);
            // Adding node to smaller leaf set
            else if i<myID  && not(smallerLeaf.Contains(i)) then
                if smallerLeaf.Count < 4 then
                    smallerLeaf.Add(i);
                else 
                    let min, minIndex=getMin(smallerLeaf);
                    if i<min then
                        smallerLeaf.RemoveAt(minIndex);
                        smallerLeaf.Add(i);
            let samePrefix = checkPrefix(toBase4String(myID, base_b), toBase4String(i, base_b)); // Performing prefix matching
            let mutable x = toBase4String(i, base_b);
            let xtonum = int(string(x.[samePrefix])); 
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
        |   InitialJoin(groupOne: List<int>) ->
                groupOne.RemoveAt(groupOne.IndexOf(myID)); //  Removes current node's ID from group one in order to make leaf set
                addBuffer(groupOne);
                for i = 0 to (base_b-1) do
                    let mutable x = toBase4String(myID, base_b);
                    let mutable xthdigit = System.Char.GetNumericValue(x.[i]) |> int
                    routingTable.[i].[xthdigit] <- myID;
                sender <! FinishedJoining
        |   Route(messageRoute: string, requestFrom: int, requestTo: int, hops: int) ->
                match messageRoute with 
                |   "Join" -> 
                    let mutable o=0;
                    let mutable p=0;
                    let mutable diff=0;
                    printfn "Inside join for myID: %d" myID
                    samePrefix <- checkPrefix(toBase4String(myID, base_b), toBase4String(requestTo, base_b));
                    indexToroutingTable <-toBase4String(requestTo, base_b);
                    rowRouting <- int(string(indexToroutingTable.[samePrefix]));
                    if hops = -1 && samePrefix>0 then // If hops =-1, then node has just joined
                        for i in [0 .. samePrefix-1] do
                            mapofinitActors.[string(requestTo)] <! AddRow(i,clone(routingTable.[i]))
                            // pastryNodes.[requestTo]<! AddRow(i,t); 
                    mapofinitActors.[string(requestTo)] <! AddRow(samePrefix,clone(routingTable.[samePrefix]))       
                    let minimum, minIndex = getMin(smallerLeaf); // Get index of smallest element in list and its index
                    let maximum, maxIndex = getMax(largerLeaf) // Get index of largest element in list and its index

                    // let mutable indexToroutingTable = toBase4String(requestTo, base_b);
                    // let rowRouting = int(string(indexToroutingTable.[samePrefix]));

                    if (smallerLeaf.Count>0 && requestTo>= minimum && requestTo<=myID) ||(largerLeaf.Count>0 && requestTo<= maximum && requestTo>=myID) then
                        diff <- idSpace+10;
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
                            mapofinitActors.[string(nearest)] <! Route(messageRoute, requestFrom, requestTo, hops+1)
                        else
                            let mutable allLeaf = new List<int>(); // Creating a leaf set
                            allLeaf.Add(myID);                    // Adding current node to leaf set
                            allLeaf <- appendLists(allLeaf,smallerLeaf);     // Adding smaller leaf set
                            allLeaf <- appendLists(allLeaf, largerLeaf);   // Adding larger leaf set
                            mapofinitActors.[string(requestTo)] <!AddLeaf(allLeaf); //Updating the leafset of the node closest to the node joining

                    else if(smallerLeaf.Count<4 && smallerLeaf.Count>0 && requestTo<minimum) then // If the smallest element in the smallerLeaf set is greater than the current node ID, route message to it
                        mapofinitActors.[string(minimum)] <! Route(messageRoute, requestFrom, requestTo, hops+1)

                    else if(largerLeaf.Count<4 && largerLeaf.Count>0 && requestTo> maximum) then // If the largest element in the largerLeaf set is smaller than the current node ID, route message to it
                        mapofinitActors.[string(maximum)] <! Route(messageRoute, requestFrom, requestTo, hops+1) 

                    else if ((smallerLeaf.Count = 0 && requestTo<myID) || (largerLeaf.Count = 0 && requestTo>myID) ) then
                        let mutable allLeaf = new List<int>();
                        allLeaf.Add(myID);
                        allLeaf <- appendLists(allLeaf,smallerLeaf);
                        allLeaf <- appendLists(allLeaf, largerLeaf);
                        mapofinitActors.[string(requestTo)] <!AddLeaf(allLeaf);      
                    
                    else if routingTable.[samePrefix].[rowRouting] <> -1 then
                        mapofinitActors.[string(routingTable.[samePrefix].[rowRouting])]<!Route(messageRoute, requestFrom, requestTo, hops+1)


                    else if requestTo>myID then
                        let m,mi = getMax(largerLeaf)
                        mapofinitActors.[string(m)]<! Route(messageRoute,requestFrom, requestTo, hops+1)
                        masterActor.[0] <! NotInBoth

                    else if requestTo<myID then
                        let o,p = getMin(smallerLeaf)
                        mapofinitActors.[string(o)]<! Route(messageRoute,requestFrom, requestTo, hops+1)
                        masterActor.[0] <! NotInBoth

                    
                    
                    else
                        printfn("Not Possible")
                |   "Route" ->
                    if myID=requestTo then
                        masterActor.[0] <! RouteFinish(requestFrom,requestTo,hops+1)
                    else 
                        samePrefix <- checkPrefix(toBase4String(myID, base_b), toBase4String(requestTo, base_b));
                        let minimum, minIndex = getMin(smallerLeaf); // Get index of smallest element in list and its index
                        let maximum, maxIndex= getMax(largerLeaf) // Get index of largest element in list and its index
                        let mutable x = toBase4String(requestTo, base_b);
                        let xtoRow = int(string(x.[samePrefix])); 

                        if (smallerLeaf.Count>0 && requestTo>= minimum && requestTo<=myID) ||(largerLeaf.Count>0 && requestTo<= maximum && requestTo>=myID) then
                            let mutable  diff = idSpace+10;
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
                            mapofinitActors.[string(minimum)]<! Route(messageRoute, requestFrom, requestTo, hops+1)

                        else if(largerLeaf.Count<4 && largerLeaf.Count>0 && requestTo> maximum) then // If the largest element in the largerLeaf set is smaller than the current node ID, route message to it
                            mapofinitActors.[string(maximum)]<! Route(messageRoute, requestFrom, requestTo, hops+1) 

                        else if ((smallerLeaf.Count = 0 && requestTo<myID) || (largerLeaf.Count = 0 && requestTo>myID) ) then
                            masterActor.[0] <! RouteFinish(requestFrom, requestTo, hops+1)  

                        else if routingTable.[samePrefix].[xtoRow] <> -1 then
                            mapofinitActors.[string(routingTable.[samePrefix].[xtoRow])] <! Route(messageRoute, requestFrom, requestTo, hops+1)

                        else if requestTo>myID then
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
                    if routingTable.[rowNum].[i] = -1 then
                        routingTable.[rowNum].[i] <- newRow.[i]
        |   AddLeaf(allLeaf) ->
                addBuffer(allLeaf);
                for i in smallerLeaf do
                    numOfBack <- numOfBack+1;
                    mapofinitActors.[string(i)]<! Update(myID);
                for i in largerLeaf do
                    numOfBack <- numOfBack+1;
                    mapofinitActors.[string(i)] <! Update(myID);
                for i in[0 .. base_b-1] do
                    for j in [0 .. 3] do
                        if routingTable.[i].[j] <> -1 then
                            numOfBack <- numOfBack+1
                            mapofinitActors.[string(routingTable.[i].[j])]<!Update(myID)
                for i in [0 .. base_b-1] do
                    let mutable x = toBase4String(myID, base_b);
                    let xtoInte =  int(string(x.[i]));
                    routingTable.[i].[xtoInte] <- myID
        |   Receive(messageReceived) ->
            match messageReceived with
            |   "StartRouting" ->
                let totalNumReuests = numRequests + int(float(numNodes) * float(0.2))
                for i in [1 .. numRequests] do
                    printfn "myID %d" myID
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

    // if numNodes <= 5000 then 
    //     groupOneSize <- numNodes
    // else 
    //     groupOneSize <- 5000

    groupOneSize <- int(0.8*float(numNodes)) // Static overlay network

    let rec masterloop() = actor{
        let! msg =  mailbox.Receive ()
        match msg with
        | Init ->
            for i = 0 to nodeIDSpace-1 do 
                randomList.Add(i)

            // for i = 0 to nodeIDSpace-1 do 
            //     swap randomList i (random.Next(randomList.Count - 1))

            for i = 0 to groupOneSize - 1 do 
                groupOne.Add(randomList.[i])
            
            for i in [0 .. numNodes-1] do
                let properties = string(randomList.[i])
                let actor = spawn Pastry properties pastryNode
                actor <! PastryInit(numNodes, numRequests, randomList.[i])// base_b)
                mapofinitActors <- mapofinitActors.Add(properties, actor)
            System.Threading.Thread.Sleep(10) // Implement using flag
            mailbox.Self <! Start
        |   Start-> 
            for i = 0 to groupOneSize-1 do
                mapofinitActors.[string(randomList.[i])] <! InitialJoin(clone(groupOne))

            
        |   FinishedJoining ->
            numJoined <- numJoined + 1
            printfn "numJoined is %d" numJoined
            if numJoined= int(0.80*float(numNodes)) then
                mailbox.Self<!StartRouting
            // if numJoined > groupOneSize then 
            //     mailbox.Self <! SecondaryJoin
            // if numJoined >= groupOneSize then 
            //     mailbox.Self <! StartRouting
            
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
                mapofinitActors.[string(startID)] <! Route("Join", startID, randomList.[numJoined], -1)
                // call the actor
        | StartRouting -> 
            // broadcast message
            for i = 0 to groupOneSize - 1 do 
                mapofinitActors.[string(randomList.[i])] <! Receive("StartRouting") 
        | RouteFinish (requestFrom, requestTo, hops)->
            numRouted <- numRouted + 1
            numHops <- numHops + hops
            if numRouted = numNodes * (numRequests) then 
                printfn "\n"
                printfn "Total Routes -> %d and Total Hops %d" numRouted numHops 
                let dummy = float(numHops) / float(numRouted)
                printfn "Average hops per Route in float-> %f" dummy
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
    let masterActorNode = spawn Pastry "master" master 
    masterActor.Add(masterActorNode)  
    masterActor.[0] <! Init
    
    while not flag do
        let mutable i = 0
        i <- i+1
    
    0
let args = fsi.CommandLineArgs 
match args.Length with //Checking number of parameters
    | 3 -> main args    
    | _ ->  main args
