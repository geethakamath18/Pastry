// #time "on"
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
// let Pastry = System.create "system" <| Configuration.defaultConfig()
// let mutable nodeGenerated = new List<int>();
let mutable numNodes=0;
let mutable numRequests=0;
let mutable nodeID=0;
let mutable masterActor=new List<IActorRef>(); // Master Actor
let mutable pastryNodes=new List<IActorRef>(); 
let mutable flag=false; 
let mutable getRandom=0;
let mutable t= new List<int>();


type ActorMessageType = 
    |   PastryInit of int * int * int * int
    |   Receive of string
    |   InitialJoin of List<int>
    |   Route of string * int * int * int
    |   AddRow of int * List<int>
    |   AddLeaf of List<int>
    |   Update of int 
    |   RouteFinish of int * int * int // Master messages
    |   Master of int * int

let PastryNode(mailbox: Actor<_>) =
    let mutable smallerLeaf = new List<int>();
    let mutable largerLeaf = new List<int>();
    let mutable numOfBack=0;
    let sender=mailbox.Sender();
    // let mutable numRequests=0;
    // let mutable numNodes=0;
    let mutable routingTable= new List<List<int>>();
    let mutable idSpace=0;
    let mutable myID=0;
    let mutable samePrefix =0;
    let mutable b=0;
    
    // Function to perform prefix matching of 2 nodeIDs
    let checkPrefix(string1: string, string2: string)=
        let mutable j=0;
        while j<string1.Length && string1.Chars(j)<>string2.Chars(j) do
            j<-j+1
        j
    

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

    // Function to append two lists, l1 is the resultant list, elements of l2 are added to l1
    let appendLists(l1 : List<int>,l2: List<int>) = 
        for i in l2 do
            l1.Add(i)
        l1

    // Function to create Leaf Set 
    let addBuffer(all: List<int>)=
        for i in all do
            // Adding node to larger leaf set
            if i>myID && not(largerLeaf.Contains(i)) then
                if largerLeaf.Count<4 then // If leaf set isn't full, add node to leaf set
                    largerLeaf.Add(i);
                else 
                    let m, mi=getMax(largerLeaf);
                    if i<m then
                        largerLeaf.RemoveAt(mi);
                        largerLeaf.Add(i);
            // Adding node to smaller leaf set
            else if i<myID  && not(smallerLeaf.Contains(i)) then
                if smallerLeaf.Count<4 then // If leaf set isn't full, add node to leaf set
                    smallerLeaf.Add(i);
                else 
                    let m, mi=getMin(smallerLeaf);
                    if i<m then
                        smallerLeaf.RemoveAt(mi);
                        smallerLeaf.Add(i);
            
            // Checking the routing table KIRIK PART
            samePrefix <- checkPrefix(toBase4String(myID, b), toBase4String(i, b)); // Performing prefix matching
            let mutable x = toBase4String(string(i), b);
            x <- int(string(x.[samePrefix])); // Index of string after which prefix differs 
            if int(string(routingTable.[samePrefix].[x]))=(-1) then
                routingTable.[samePrefix].[x]<-i   // Addinhg entries to routing table if it is empty

    // Function to add node
    let addNode(node: int)=
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
            samePrefix <- checkPrefix(toBase4String(myID, b), toBase4String(i, b));
            let mutable x = toBase4String(string(i), b);
            x <- int(string(x.[samePrefix]));
            if int(string(routingTable.[samePrefix].[x]))=(-1) then
                routingTable.[samePrefix].[x]<-i 
    
    //Pastry Node actor loop    
    let rec loop () = actor {
        let! msg = mailbox.Receive ();
        match msg with
        |   PastryInit(nodes, requests, identify, baseRecieved) ->
                let mutable temp=new List<int>();
                b <- baseRecieved;
                for i in[0..3] do
                    temp.Add(-1);
                idSpace <- int(4.00**float(b)); 
                for i in [0 .. b] do
                    routingTable.Add(temp);  
                // numNodes <- n;
                // numRequests <- requests;
        |   InitialJoin(groupOne: List<int>) ->
                groupOne.RemoveAt(groupOne.IndexOf(myID)); //  Removes current node's ID from group one in order to make leaf set
                addBuffer(groupOne);
                for i in [0 . . b] do
                    let mutable x = toBase4String(myID, b);
                    x <- int(x.[i]);
                    routingTable.[i].[x] <- myID; 

        |   Route(messageRoute, requestFrom, requestTo, hops) ->
            match messageRoute with
            |   "Join" ->
                let mutable o=0;
                let mutable p=0;
                let mutable diff=0;
                letmutable nearest=-1;
                samePrefix <- checkPrefix(toBase4String(myID, b), toBase4String(requestTo, b));
                if hops =-1 && samePrefix>0 then // If hops =-1, then node has just joined
                    for i in [0 . . samePrefix] do
                        t <- routingTable.[i]
                        pastryNodes[requestTo]<! AddRow(i,t); // Adding rows to routing table to nearest node

                m, mi= getMin(smallerLeaf); // Get index of smallest element in list and its index
                o, p= getMax(largerLeaf) // Get index of largest element in list and its index
                if (smallerLeaf.Count>0 && requestTo>= m && requestTo<=myID) ||(largerLeaf.Count>0 && requestTo<= o && requestTo>=myID) then
                    diff <- idSpace+10;
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
                        let mutable allLeaf = new List<int>(); // Creating a leaf set
                        allLeaf.Add(myID);                    // Adding current node to leaf set
                        allLeaf <- appendLists(allLeaf,smallerLeaf);     // Adding smaller leaf set
                        allLeaf <- appendLLists(allLeaf, largerLeaf);   // Adding larger leaf set
                        pastryNodes.[requestTo] <!AddLeaf(allLeaf); //Updating the leafset of the node closest to the node joining
                
                o,p <- getMin(smallerLeaf) 
                else if(smallerLeaf.Count<4 && smallerLeaf.Count>0 && requestTo<o) then // If the smallest element in the smallerLeaf set is greater than the current node ID, route message to it
                    pastryNodes.[o]<! Route(messageRoute, requestFrom, requestTo, hops+1)
                
                o,p <- getMax(largerLeaf)   
                else if(largerLeaf.Count<4 && largerLeaf.Count>0 && requestTo> o) then // If the largest element in the largerLeaf set is smaller than the current node ID, route message to it
                    pastryNodes.[o]<! Route(messageRoute, requestFrom, requestTo, hops+1)
                
                else if ((smallerLeaf.Count = 0 && requestTo<myID) || (largerLeaf.Count = 0 && requestTo>myID) ) then
                    let mutable allLeaf = new List<int>();
                    allLeaf.Add(myID);
                    allLeaf <- appendLists(allLeaf,smallerLeaf);
                    allLeaf <- appendLLists(allLeaf, largerLeaf);
                    pastryNodes.[requestTo] <!AddLeaf(allLeaf);

                let mutable x = toBase4String(requestTo, b);
                x <- int(string(x.[samePrefix]));
                else if routingTable.[samePrefix].[x]!=-1 then
                    pastryNodes.[routingTable.[samePrefix].[x]]<!Route(messageRoute, requestFrom, requestTo, hops+1)

                else if requestTo>myID then
                    m,mi <- getMax(largerLeaf)
                    pastryNodes.[m]<! Route(messageRoute,requestFrom, requestTo, hops+1)
                    sender<! NotInBoth

                else if requestTo<myID then
                    o,p <- getMin(smallerLeaf)
                    pastryNodes.[o]<! Route(messageRoute,requestFrom, requestTo, hops+1)
                    sender<! NotInBoth
                
                else
                    printfn("Not Possible")

            |   "Route" ->
                let mutable o=0;
                let mutable p=0;
                let mutable diff=0;
                letmutable nearest=-1;
                if myID=requestTo then
                    sender <! RouteFinish(requestFrom,requestTo,hops+1)
                else 
                    samePrefix <- checkPrefix(toBase4String(myID, b), toBase4String(requestTo, b));
                    m, mi= getMin(smallerLeaf); // Get index of smallest element in list and its index
                    o, p= getMax(largerLeaf) // Get index of largest element in list and its index
                    if (smallerLeaf.Count>0 && requestTo>= m && requestTo<=myID) ||(largerLeaf.Count>0 && requestTo<= o && requestTo>=myID) then
                        diff <- idSpace+10;
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
                           sender<! RouteFinish(requestFrom, requestTo, hops+1)
                    o,p <- getMin(smallerLeaf) 
                    else if(smallerLeaf.Count<4 && smallerLeaf.Count>0 && requestTo<o) then // If the smallest element in the smallerLeaf set is greater than the current node ID, route message to it
                        pastryNodes.[o]<! Route(messageRoute, requestFrom, requestTo, hops+1)
                    
                    o,p <- getMax(largerLeaf)   
                    else if(largerLeaf.Count<4 && largerLeaf.Count>0 && requestTo> o) then // If the largest element in the largerLeaf set is smaller than the current node ID, route message to it
                        pastryNodes.[o]<! Route(messageRoute, requestFrom, requestTo, hops+1)
                    
                    else if ((smallerLeaf.Count = 0 && requestTo<myID) || (largerLeaf.Count = 0 && requestTo>myID) ) then
                        sender<! RouteFinish(requestFrom, requestTo, hops+1)

                    let mutable x = toBase4String(requestTo, b);
                    x <- int(string(x.[samePrefix]));
                    else if routingTable.[samePrefix].[x]!=-1 then
                        pastryNodes.[routingTable.[samePrefix].[x]]<!Route(messageRoute, requestFrom, requestTo, hops+1)

                    else if requestTo>myID then
                        m,mi <- getMax(largerLeaf)
                        pastryNodes.[m]<! Route(messageRoute,requestFrom, requestTo, hops+1)
                        sender<! NotInBoth

                    else if requestTo<myID then
                        o,p <- getMin(smallerLeaf)
                        pastryNodes.[o]<! Route(messageRoute,requestFrom, requestTo, hops+1)
                        sender<! NotInBoth
                    
                    else
                        printfn("Not Possible")

                    

            | _ -> printfn"Unknown message"

        |   AddRow(rowNum, newRow) ->
                for i in [0 ..3] do
                    if routingTable.[rowNum].[i] = -1 then
                        routingTable.[rowNum].[i] <- newRow.[i];


        |   AddLeaf(allLeaf) ->
                addBuffer(allLeaf);
                for i in smallerLeaf do
                    numOfBack <- numOfBack+1;
                    pastryNodes.[i]<! Update(myID);
                
                for i in largerLeaf do
                    numOfBack <- numOfBack+1;
                    pastryNodes.[i] <! Update(myID);

                for i in[0.. b] do
                    for j in [0 .. 3] do
                        if routingTable.[i].[j] routingTable.[i].[j] != -1 then
                            numOfBack <- numOfBack +1
                            pastryNodes.[routingTable.[i].[j]] <!Update(myID)
                
                for i in [0..b] do
                    let mutable x = toBase4String(myID, b);
                    x <- int(x.[i]);
                    routingTable.[i].[x] <- myID


        |   Update(newNodeID) ->
                addNode(newNodeID);
                sender<!"Acknowledgement"
        |   Receive(messageReceived) ->
                match messageReceived with
                |   "StartRouting"
                    for i in [1.. numRequests] do
                        System.Threading.Thread.Sleep(1000);
                        getRandom <- random.Next(idSpace);
                        pastryNodes[getRandom]<!Route("Route", myID, getRandom, -1);

                |   "Acknowledgement"
                        numOfBack <- numOfBack-1;
                        if numOfBack <> 0 then
                            sender<!"FinishedJoining"

                |   "DisplayLeafAndRouting" -> 
                        display();
                        flag=true;
                |   _ -> printfn "Wrong Message"
        return! loop ()
    }
    loop ()               

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
            let properties = string(i)
            let actor = spawn system properties PastryNode
            pastryNodes.Add(actor)
            // spawn the actors of type Pastry

        match msg with
        | "Start" -> 
            for i = 0 to groupOneSize
                pastryNodes.[randomList.[i]]<! InitialJoin(groupOne)                
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
