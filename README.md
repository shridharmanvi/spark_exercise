### An exercise on Apache spark

This is an application to read three different files and perform data cleansing/processing operations on it using apache spark.

##### Reading html files data:
1. Html files are read completely and only selected fields are loaded into RDD instead of whole file to avoid unwanted use of memory
2. Hence the file is pre processed by using buffered reader to ensure we do not run into memory issues while reading the huge file 
3. I decided not to use an html parser since most of the items that I needed were outside the html DOM (within javascript block). So using string matching made more sense

##### Future improvements:
1. Running the project on cluster mode will improve the speed 
2. The html parsing process can be made faster by using a thread pool to perform that action (or it may act as a bottle neck to the whole app)

