## 项目简单描述
+ 使用DTS同时获取多个数据源的增量数据
+ 多线程对数据进行合并处理
+ 使用jdbc对合并后的数据写回到数据库

## 开发环境以及其他依赖
+ JDK 1.6+
+ Maven 3.2+
+ Eclipse / InteliJ IDEA / 其他IDE
+ 项目依赖的Maven: [最新版本查询](http://search.maven.org/#search%7Cga%7C1%7Ccom.aliyun.dts)


## 如何编译、运行、使用示例项目
+ 配置日志输出文件，log4j.properties和log4j.xml两种形式保留一个文件即可，将另一个文件重命名即可
+ 打包成可执行的jar包：
  - mvn clean assembly:assembly -Dmaven.test.skip=true
  - 或者 mvn clean package -Dmaven.test.skip=true
+ 运行命令：
  - cd target && java -jar demo-1.0-SNAPSHOT-jar-with-dependencies.jar 

## TODO
+ 使用log4j进行格式化的日志输出
+ 处理SyncOperation里primary keys
+ 输出SQL
