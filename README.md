# IPDataProject

Count all browsers by types. Categorize data by ip - average  bites and total.
RawComparator is implemented.
Commands 
Put list of files into hadoop:
 hadoop fs -put access_logs/000000 input

Command to execute jar:
 hadoop jar IPDataProject.jar IPDataDriver <input_file> <output_file> <type_of_file("seq" or "csv")>

Information is printed in sequence file by default.

Move output file to csv format:
 hadoop fs -cat <output_file>/part-r-00000 > /*/IPDataProject/<output_file>.csv

Move output sequence file to decompressed view:
 hadoop fs -libjars IPDataProject.jar -text output/part-r-00000



