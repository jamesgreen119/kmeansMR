import os
import numpy as np

## Calculate minima:
os.system("rm src/main/java/com/msia/app/*")
os.system("cp Min.java src/main/java/com/msia/app/Min.java")
os.system("mvn package")
os.system("yarn jar target/mr-app-1.0-SNAPSHOT.jar Min hw3/sample.txt hw3/MinF")

## Calculate maxima:
os.system("rm src/main/java/com/msia/app/*")
os.system("cp Max.java src/main/java/com/msia/app/Max.java")
os.system("mvn package")
os.system("yarn jar target/mr-app-1.0-SNAPSHOT.jar Max hw3/sample.txt hw3/MaxF")

## Create mins-maxes file:
os.system("hdfs dfs -cat hw3/MinF/* hw3/MaxF/* > Minmax.txt")

## Create standardized dataset:
os.system("rm src/main/java/com/msia/app/*")
os.system("cp Minmax.java src/main/java/com/msia/app/Minmax.java")
os.system("mvn package")
os.system("yarn jar target/mr-app-1.0-SNAPSHOT.jar Minmax hw3/sample.txt hw3/standardized hw3/Minmax.txt")




## K-Means loop:
numVariables = 5
numIterations = 20
K = 5



randomVector = np.random.rand(K,numVariables+1)
np.savetxt("centroids.txt",randomVector,delimiter="\t")

for i in range(0,numIterations):
    os.system("rm .centroids.txt.crc")
    os.system("cat centroids.txt")
    os.system("hdfs dfs -rm -r hw3/centroids/centroids.txt")
    os.system("hdfs dfs -put centroids.txt hw3/centroids/")
    os.system("hdfs dfs -rm -r hw3/output")
    os.system("yarn jar target/mr-app-1.0-SNAPSHOT.jar kmeans hw3/standardized hw3/output "+str(K))
    os.system("rm centroids.txt")
    os.system("hdfs dfs -getmerge hw3/output centroids.txt")
