# StreamX
All test files are located in src/main/scala

Third Test was the initial trial and error stuff to get streaming and GraphX working together
Delete test was the initial set up of the subgraph methods etc. 
Currently working on getting the temporal stuff to fully work.

to submit the classes for test:
spark-submit --class "DeleteTest" target/scala-2.10/network-word-count_2.10-1.0.jar localhost 9999

where "DeleteTest" can be replaced with whatever class you are trying

the localhost 9999 at the end is what address/port the program should listen on, so can be changed to anything.
I have been using "nc -lk 9999" on linux as it means I can just have it in another terminal tab 

To build it if you many any changes, you can simply use "sbt package" as the sbt build file
is in the main directory.

The current version accepts data in the form "command sourceNode edgeConnection destNode" for example:
addEdge 4 knows 5

From DeleteTest the commands consist of:
addEdge src edg dest
addNode id
rmvEdge src edg dest
rmvNode id
