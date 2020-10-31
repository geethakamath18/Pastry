#time "on"
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Akka.TestKit

open Akka.FSharp

// Message types for worker and the supervisor actors
type ProcessorMessage = ProcessJob of int * int * int
type MasterMessage = MasterJob of int * int * int * int

//Creating the Actor System
let system = System.create "system" <| Configuration.defaultConfig()


//Global Counter
let mutable counter = 0

//Creating 2000 actors for every logical core
let coreCount = Environment.ProcessorCount
let mutable numberOfCores= 2000*coreCount

//Type of message recieved by actor
type ActorMessageType = 
    | Message of string

//Function to check if a number is a perfect square
let squareRoot (t: uint64) = 
    let z = t |> float
    let x = sqrt z
    abs(x % float(1)) < System.Double.Epsilon


//Function to compute squares of a range of numbers that forma perfect square
let computeSquare (x: uint64, y: uint64 ,z )= 
    for i = int(x) to int(y) do
        let mutable t = 0 |> uint64
        for j = i to i+z-1 do 
            t <- t + uint64(j) * uint64(j)
        if (squareRoot t) then 
            printfn "%d " i

//Worker actor definition
let echo (mailbox:Actor<_>) =
    let rec loop () = actor {
        let! ProcessJob(x, y, z) = mailbox.Receive () //Recieving messages from the mailbox
        let sender = mailbox.Sender() //Getting the sender
        computeSquare(uint64(x),uint64(y), z)
        sender <! Message "message recived" //Indicating that the worker actor actor has finished executing asynchronously
        return! loop ()
    }
    loop ()

//Getting Command Line Arguements
let args = fsi.CommandLineArgs 

//Master actor defintiion
let master (mailbox: Actor<_>) = 
    let rec masterloop() = actor{

       
        let n=(int) args.[1] //Setting the value of n
        let k=(int) args.[2] //Setting the value of k

        let range= (n/numberOfCores) //Setting the value of range for each actor
        let arange=(ceil (range|>float))|>int
        
        let mutable s=1 //Variable for beginning of range
        let mutable e=arange //Variable for ending of range
        
        let echoActors =        //Spawning Actors
            [1 .. numberOfCores]
            |> List.map(fun id ->   let properties = string(id) 
                                    spawn system properties echo)

        for id in [0 .. numberOfCores-1] do
            if id = numberOfCores-1 then
                (id) |> List.nth echoActors <! ProcessJob(e+1, n, k) //Sending messages to last actor
            else 
                (id) |> List.nth echoActors <! ProcessJob(s, e, k) //Sending messages to actors
            s <- e + 1
            e <- e + arange
            let! msg  =  mailbox.Receive () //Recieving messages from worker actors
            match msg with //Pattern Matching
            |Message msg ->
                counter <- counter+1 //Incrementing the global counter
        return! masterloop ()
    }      
    masterloop() 

//main function
let main (args:string []) =
    let mutable flag=true
    let masterActor = spawn system "master" master //Spawning the master/Supervisor actor
    masterActor <! "Start Master Actor"

    while flag do 
        if numberOfCores=counter then
            flag<-false
    0 

   
match args.Length with //Checking number of parameters
    | 3 -> main args    
    | _ ->  failwith "You need to pass two parameters!"
