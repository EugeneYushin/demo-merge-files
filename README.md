# demo-merge-files


This example shows how to use MapReduce paradigm for merging lines from different files. Main disadvantage is applying one mapper per input file without splitting to prevent skewness of lines into Reduce stage.
 
It's better to implement custom InputFormat class for this scenario.
