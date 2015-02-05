#!/usr/bin/R
args<-commandArgs(TRUE)
fileName<-args[1]
data=read.table(fileName)[,2]
quantile(data, c(.25, .50,  .75, .90, .95, .99))
print("Mean = ")
mean(data, trim=0.05)
