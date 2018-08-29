# ImoocLog_SparkSQL
慕课网日志处理SparkSQL版<br/>
需求一：统计慕课网最受欢迎的TopN课程；<br/>
需求二：按省市统计慕课网最受欢迎的TopN课程；<br/>
需求三：按流量统计慕课网最受欢迎的TopN课程。<br/>
数据处理流程：<br/>
1）数据采集<br/>
	Flume： web日志写入到HDFS<br/>

2）数据清洗<br/>
	脏数据<br/>
	Spark、Hive、MapReduce 或者是其他的一些分布式计算框架<br/>  
	清洗完之后的数据可以存放在HDFS(Hive/Spark SQL)<br/>

3）数据处理<br/>
	按照我们的需要进行相应业务的统计和分析<br/>
	Spark、Hive、MapReduce 或者是其他的一些分布式计算框架<br/>

4）处理结果入库<br/>
	结果可以存放到RDBMS、NoSQL<br/>

5）数据的可视化<br/>
	通过图形化展示的方式展现出来：饼图、柱状图、地图、折线图<br/>
	ECharts、HUE、Zeppelin<br/>
 
 本项目主要做了2-4步骤。<br/>
