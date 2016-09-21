# solr-dih-redis
A Simple Solr Data Import Handler (DIH) plugin to read from Redis.

Setup
 
##### Sample solrconfig.xml
```xml
  <!-- 
        Note: Copy all your DIH Jars to the second path. 
        To pull data from Redis, you will NEED to copy  the following: 
            1) commons-pool2-x.x.x.jar
            2) jedis-x.x.x.jar 
            3) solr-dih-redis-x.x.x.jar
   -->
  <lib dir="${solr.install.dir:../../../..}/dist/" regex="solr-dataimporthandler-.*\.jar"/>
  <lib dir="${solr.install.dir:../../../..}/contrib/dataimporthandler/lib" regex=".*\.jar"/>
```
 
##### Sample data-config.xml

```xml
<?xml version="1.0" encoding="UTF-8" ?>
 <dataConfig>
   <dataSource
       type="JdbcDataSource"
       driver="com.mysql.jdbc.Driver"
       url="jdbc:mysql://some_mysql_server:3306/ptn?zeroDateTimeBehavior=convertToNull"
       user="user"
       password="password"
       name="mysql"/>
       
  <dataSource
      type="RedisDataSource"
      name="redis"
      host="localhost"
      port="6379"
      ssl="false"/>
 
   <document>
     <!-- Root Table -->
     <entity
         dataSource="mysql"
         processor="SqlEntityProcessor"
         name="foo"
         pk="id"
         query="SELECT * FROM foo t"
         deltaImportQuery="SELECT * FROM foo t WHERE t.id = '${dataimporter.delta.id}'"
         deltaQuery="SELECT t.id FROM foo t WHERE t.updated_at > '${dataimporter.last_index_time}'">
       <field column="id" name="id"/>
       <field column="bar" name="bar_s"/>
 
       <!-- Dependent Key/Value Pairs -->
       <entity
           dataSource="redis"
           processor="RedisEntityProcessor"
           name="baz"
           key="some:really:long:key:namespace:${foo.id}">
         <field column="value" name="value_ss"/>
         <field column="key" name="key_ss"/>
       </entity>
   </entity>
 </document>
</dataConfig>
```


TODOs:
    1. Maybe add some way to support deltas from Redis.
    2. Add unit tests.. Sorry TDD.
    3. Connection pooling and other performance stuff.