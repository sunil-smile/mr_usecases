# mr_usecases

Word count jar command
----------------------
hadoop jar wordcount-0.0.1-SNAPSHOT.jar com.sunilapp.wordcount.WordCountMR -D yarn.log-aggregation-enable=true -D field_pos=4 20151101 /input/wcinput/input.txt /output/wcoutput/13/

hadoop jar wordcount-0.0.1-SNAPSHOT.jar -D mapreduce.output.textoutputformat.separator=";" com.sunilapp.customwritable.WCompMR wc1 /input/secsortinput.txt /output/wcoutput/1/

Distributed Cache file
----------------------

hadoop jar wordcount-0.0.1-SNAPSHOT.jar com.sunilapp.mapsidejoin.JoinMR -files input.txt wc1 /input/wcinput/input.txt /output/wcoutput/31/

Secondary Sort
--------------

http://www.bigdataspeak.com/2013/02/hadoop-how-to-do-secondary-sort-on_25.html

hadoop jar wordcount-0.0.1-SNAPSHOT.jar com.sunilapp.secondarysort.SecondarySortDriver /input/secsort/secsortinput.txt /output/13/


DScache file : /tmp/hadoop-huser/nm-local-dir/usercache/huser/appcache/application_1443115485185_0007/container_1443115485185_0007_01_000002/input.txt


Seq file example
----------------

> 2 types recor, block

> merging small files to seq 

- 

key               value

filepath         files content


hadoop jar wordcount-0.0.1-SNAPSHOT.jar com.sunilapp.seqfileexample.TextToSequenceDriver /tmp/simplyanalytics/input/employee.txt /tmp/simplyanalytics/input/21_1/

htxt /output/sqoutput/1_1/part*

hadoop jar wordcount-0.0.1-SNAPSHOT.jar com.sunilapp.seqfileexample.SequenceToTextDriver /output/sqoutput/2_1/ /output/sqoutput/2_2/

hcat /output/sqoutput/1_2/part*

hadoop jar wordcount-0.0.1-SNAPSHOT.jar com.sunilapp.seqfileexample.TextToBlckCompSequenceDriver /output/sqoutput/1_2/ /output/sqoutput/1_3/

hadoop jar wordcount-0.0.1-SNAPSHOT.jar com.sunilapp.seqfileexample.SmallFilesToSequence /input/wcinput/ /output/3

KV Input
--------
-files 
-libjar
-D
-conf


hadoop jar wordcount-0.0.1-SNAPSHOT.jar com.sunilapp.inputformats.KVInputDriver -D noofcolms=false -D mapreduce.output.textoutputformat.separator=";" /input/wcinput/kvinput.txt /output/wcoutput/5/

NLineInputFormat
-----------------

hadoop jar wordcount-0.0.1-SNAPSHOT.jar com.sunilapp.inputformats.NLineinputDriver -conf input.conf /input/wcinput/kvinput.txt /output/wcoutput/52/

Secondary Sort
--------------

http://www.bigdataspeak.com/2013/02/hadoop-how-to-do-secondary-sort-on_25.html

hadoop jar wordcount-0.0.1-SNAPSHOT.jar com.sunilapp.secondarysort.SecondarySortDriver /input/secsort/secsortinput.txt /output/13/




chain MR
--------

hadoop jar wordcount-0.0.1-SNAPSHOT.jar com.sunilapp.chainmr.ChainWordCountDriver -conf threshold_value=2 /tmp/simplyanalytics/input/wordcount.txt /tmp/simplyanalytics/output/1/


MultiInputMR
-------------
hadoop jar wordcount-0.0.1-SNAPSHOT.jar com.sunilapp.multiinput.MultiInputMR /input/secsort/secsortinput.txt,/input/wcinput/input.txt /output/14/


MultiOutput MR
---------------

hadoop jar wordcount-0.0.1-SNAPSHOT.jar com.sunilapp.multioutput.MultiOutputDriver /input/secsort/secsortinput.txt /output/14/
