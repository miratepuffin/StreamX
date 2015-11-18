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

For TemporalTest there is the new command of loadOld -
this takes in a date time in the format:
loadOld yyyy-MM-dd HH:mm:ss
and will search the prev folder for the closest graph to this date - setting it as the used graph

to run the latest version:
spark-submit --class "nonTailParseTest" target/scala-2.10/network-word-count_2.10-1.0.jar "additionalProjects/generator/generatedData"
this will attach it to the folder the generator works in

once this is running and processing batches
open a new terminal and navigate to additionalProjects/generator and run the gen (scala writeOutTest)
You may want to cancel it after the first right out, so only one block of data is read in
(I think the current generator is set to only write out 10, so you may want to change that to 100 or something, just se the number in the while loop)
Once this is done you will see either a stackoverflow or the itteration that reads this in will freeze once it attempts to save/print any of the data
