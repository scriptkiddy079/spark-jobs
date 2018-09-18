# Spark_Scala-Maven-Set-Up
This includes the set up of spark 2.3.0 with full spark libraries dependencies in maven
###########################################################################################################################################



1. In Eclipse go to Windows-->Preferences-->Maven-->Archetype-->Add Remote Catalog
2.Fill (Catalog File:) with--> http://repo1.maven.org/maven2/archetype-catalog.xml
3. Add any description.
4. create a maven project searching* any 'spark-scala' archetype.
5.Add the dependencies available in the pom.xml from this repository.
6.Add the class file in src/main/scala
7. Download winutils.exe(Hadoop binary folder) and add it to bin(create new folder with this name) folder of your project.
 *Must check compatibility of spark with Hadoop Binary.
8.*Scala version must be 2.11(dynamic)


