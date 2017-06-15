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
