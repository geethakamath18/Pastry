
open System
// #r "nuget: Akka.FSharp" 
// #r "nuget: Akka.TestKit" 

open System
open System.Collections.Generic
let mutable temp=new List<int>();
let mutable t1=new List<int>();
let mutable t2=new List<int>();
let mutable res= new List<int>();
let mutable intermediateRes= new List<int>();
// let getMax()=
t1.Add(1);
t1.Add(3);

t2.Add(7);
t2.Add(20);

temp.Add(200);
temp.Add(70);
temp.Add(140);
temp.Add(900);
let appendLists(l1 : List<int>,l2: List<int>) = 
    for i in l2 do
        l1.Add(i)
    l1
// res<-appendLists(res,temp)
// res<-appendLists(res,t1)
// res<-appendLists(res,t2)

// res <- res @ t1

for i in res do
    printfn "%A" i
//     let mutable j=0;
//     let mutable max=0;
//     let mutable maxIndex=(-1);
//     for i in temp do
//         if i>max then
//             max<-i;
//             maxIndex <-j;
//         j <- j+1;
//     max, maxIndex
       
// let m, mi=getMax();
// printfn "%d %d" m mi
// // temp.RemoveAt(mi)
// printfn "%A" (temp.IndexOf(m));
// // temp.RemoveAT(maxIndex);
// printfn "%A" temp
// temp.RemoveAt(temp.IndexOf(m))
// printfn "Removed"
// printfn "%A" temp

// let mutable routingTable= new List<List<int>>();
// for i in[0..3] do
//     routingTable.Add(temp);
// printfn "%A" routingTable

// let k=999;
// let s=Convert.ToString(k, 4);
// printfn "%A" 
// let convert(value: int, targetBase: int)=
//     let mutable res="";
//     // let targetBase=;
//     let mutable value=value;
//     res<-string "0123456789ABCDEF".[value%targetBase]+res;
//     value<-value/targetBase;
//     while value>0 do
//         res<-string "0123456789ABCDEF".[value%targetBase]+res;
//         value<-value/targetBase;
//     res

// let s=convert(999,4);
// printfn "%A" s 

// let a='z';
// let b='y'
// if a<>b then
//     printfn "True"
// printfn "%A" a<>b;
// let a=4;
// let b=2;
// let s= abs(b-a);
// printfn "%A" s

