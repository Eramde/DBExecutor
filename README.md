# DBExecutor
Simple Java SQL Execution library.
Advantages:
  - Lightweight
  - Simple
  - Java SE (not requires EE)

# Usage
## Simple call
You can simply use the library like this:
```java
DBUtility dbu = DBUtility.create("jdbc.driver.Class")
    .setUrl("jdbc:somejdbcurl")
    .setCredentials("user", "password"); // or null
ArrayList<HashMap<String, Object>> result = dbu.executeQuery("select * from table where id = ? or name like ?", 
    10, "%value%");
```
Here result HashMap - is key-value presentation of single line.
## Out parameters
You also can set OUT parameters by passing `DBParameter` arguments into `executeCall` function like this:
```java
DBParameter in = new DBParameter("some value"), 
    out = new DBParameter(); //without argument, parameters is OUT by default
dbu.executeCall("begin\n ? := someFunction(?) \n end;", out, in);
System.out.println("Out value is " + out.getData());
```
## Manual connection control
By calling `executeQuery` or `executeCall`, library executes statements in try-with-resources context,
so connection will be closed after database accepts operation (and data has beed recieved).
If you need to perform some actions within one context, you can create connection, 
pass it into library function and control it by yourself like this:
```java
DBUtility dbu = DBUtility.create(null);
Connection connection = null;
try {
    connection = getConnection(); //Drivermanager.getConnection or something else
    connection.setAutoCommit(false);
	dbu.execute(connection, 
	            true, // false - executing statement like SELECT, true - call like UPDATE or procedure
	            "ALTER SESSION SET CURRENT_SCHEMA = WORK");
    dbu.execute(connection, true, "update some_table set updated_date = ? where id = ?", 
                                    new DBParameter(new Date()), new DBParameter(1));
    dbu.execute(connection, true, "update another_table set created_by = ? where id = ?", 
                                    new DBParameter("some_user"), new DBParameter(1));
    connection.commit();
} finally {
    if (connection != null) {
	    connection.close();
	}
}
```
## Data type conversion customization
By default, library has mapping between these Java classes and SQL types:
* String - VARCHAR
* BigDecimal - NUMERIC
* Boolean - BIT
* Byte - TINYINT
* Short - SMALLINT
* Integer - INTEGER
* Long - BIGINT
* Float - REAL
* Double - DOUBLE
* byte[] - VARBINARY
* Date - DATE
* Time - TIME
* Timestamp - TIMESTAMP

But you can also add your custom map for classes (if jdbc driver support it) or replace existing by calling function `setCustomSqlType`:
```java
DBUtility dbu = DBUtility.create(null)
    .setCustomSqlType(BigInteger.class, Types.BIGINT);
...
dbu.executeQuery("select * from some_table where id = ?", new BigInteger("922337295834129859545498036854775807"));
```
Library will set presented parameter as BIGINT SQL type, but there is no guarantees, that jdbc driver will pass it to DB.

Also you can add inline converter for SQL data, which has been **recieved** from DB (library can only convert only returned data and OUT parameters). 
For example, you need to convert number to String or string to Date or something else, so, you create custom `DBTypeConverter`
and register it in `setTypeConverter` function:
```java
DBUtility dbu = DBUtility.create(null)
    .setTypeConverter(java.util.Date.class, new DBTypeConverter() {
        @Override
        public String convert(Object d, String columnName) {
            if(null != columnName){ // columnName of OUT parameters is NULL
                return String.valueOf(d);
            }
            else{
                return d;
            }
        }
    });
...
ArrayList<HashMap<String, Object>> result = dbu.executeQuery("select \"date_column\" from some_table");
if(!result.isEmpty()) { //list returned from function without exceptions is not null a-priory
    HashMap<String, Object> line = result.get(0);
    if(!line.isEmpty()){ // same for Map
        Object o = line.get("date_column");
        // String if date_column is instance of java.util.Date or subtype
        System.out.println("Type of data is " + (o == null? null : o.getClass().getName()));
        System.out.println("Data value is " + o);
    }
}
```
By default, there is converter for ResultSet type (if database returned something like CURSOR).
**Be aware**: If there is no map for _EXACTLY_ same class, library will try to find converter by cast checking.
For example: 
JDBC driver returned value in java.sql.Timestamp wrapper, 
BUT there is no converter for Timestamp 
AND there is converter for java.util.Date class (which is superclass for Timestamp)
SO library will use converter which associated with java.util.Date.

## Other
### setFixAutoCommitIssue
Sometimes for correct data fetch connection should disable auto commit and manual call commit after data has been recieved.
Example: when you recieve `REFCURSOR` from function in PostgreSQL database and trying to read data from inner 
`ResultSet` (jdbc driver converts refcursor to this structure) you will get something like 'Connection already closed'.
So you need or control connection by yourself (don't close in until you read all data from refcursor) or set
`setFixAutoCommitIssue(true)` to do it automatically.
### setNullAsSubstitution
Dirty method for setting `NULL` as parameter. I do not recommend to use it, if you don't sure, that your request
will not be broken.
Problem: when you trying to call `PreparedStatement.setNull(int parameterIndex, int sqlType)`, you should to set
correct sqlType as second argument. For Java there is no difference if `null` is String or Integer class, so library cannot 
automatically find map for parameter, if null is present. So there is two ways:
1. leave it as is, so library will pass `setNull(index, Types.NULL)`, which not works with some drivers;
2. call `setNullAsSubstitution(true)`.

In last case library will modify request and replace `?` (parameter place) by string `null`. 
Example:
```java
String name = getName(); //returnes null
dbu.setNullAsSubstitution(true);
dbu.executeQuery("select * from table where id = ? or name = ?", 10, name);
```
In this example function `getName()` returns null, so library will modify request to this:
```sql
select * from table where id = ? or name = null
```
If you use literal `?` somewhere in your request, there will be problem, because library replaces this char, and I don't know how to fix it.

# License
Copyright (c) 2016, sot
All rights reserved.
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
