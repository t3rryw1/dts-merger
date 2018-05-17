## 项目简单描述
+ 使用mysql-binlog-connector-java库同时获取多个数据源的增量数据
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
+ ~~处理SyncOperation里primary keys~~
+ ~~输出SQL~~
    * ~~测试多种情况下的 SQL 输出是否正确~~
    * ~~将数据写回至数据库~~
+ ~~使用log4j进行格式化的日志输出~~
+ ~~保存每个 db 的binlog 位点到 redis，下次启动使用位点启动，避免丢失数据改动~~
+ ~~启动时读取db 的 schema，并在接收每条数据时使用 schema 生成正确的 synctask~~
+ ~~本地验证数据库同步逻辑正确。~~

