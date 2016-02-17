#!/usr/bin/python3


import sys
import os
from subprocess import call

jarPath = "target/bespin-0.1.0-SNAPSHOT.jar"
baseCP = "io.bespin."
javaCP = baseCP + "java"
scalaCP = baseCP + "scala"

javaOut = "javaOut"
scalaOut = "scalaOut"

def compile():
    print("Compiling...")
    call(["mvn","package"])

def run_java(pathSuffix, inputFile, textOutput):
    path = "%s.%s" % (javaCP, pathSuffix)
    if textOutput:
        call(["hadoop", "jar", jarPath, path, "-input", inputFile, "-output", javaOut, "-reducers", "5", "-textOutput"])
    else:
        call(["hadoop", "jar", jarPath, path, "-input", inputFile, "-output", javaOut, "-reducers", "5"])

def run_scala(pathSuffix, inputFile, textOutput):
    path = "%s.%s" % (scalaCP, pathSuffix)
    if textOutput:
        call(["hadoop", "jar", jarPath, path, "--input", inputFile, "--output", scalaOut, "--reducers", "5", "--textOutput"])
    else: 
        call(["hadoop", "jar", jarPath, path, "--input", inputFile, "--output", scalaOut, "--reducers", "5"])

def run_comp_local(pathSuffix):
    call("cat " + javaOut + "/* | sort -k 1 -n -r > javaFile", shell=True)
    call("cat " + scalaOut + "/* | sort -k 1 -n -r > scalaFile", shell=True)
    print("Total number of lines in java output:")
    call("wc -l javaFile", shell=True)
    print("Total number of lines in scala output:")
    call("wc -l scalaFile", shell=True)
    print("Differences:")
    call("diff javaFile scalaFile | head -n 10", shell=True)

def run_comp_cluster(pathSuffix):
    call("hadoop fs -cat " + javaOut + "/* | sort -k 1 -n -r > javaFile", shell=True)
    call("hadoop fs -cat " + scalaOut + "/* | sort -k 1 -n -r > scalaFile", shell=True)
    print("Total number of lines in java output:")
    call("wc -l javaFile", shell=True)
    print("Total number of lines in scala output:")
    call("wc -l scalaFile", shell=True)
    print("Differences:")
    call("diff javaFile scalaFile | head -n 10", shell=True)

if __name__ == "__main__":

    try:
        if len(sys.argv) < 3:
            print("Usage: "+ sys.argv[0] + " [classpath-suffix] [input-file] (-textOutput) ")
            exit(1)
        compile()
        if len(sys.argv) > 3 and sys.argv[3] == "-textOutput":
            run_java(sys.argv[1], sys.argv[2], True)
            run_scala(sys.argv[1], sys.argv[2], True)
        else:
            run_java(sys.argv[1], sys.argv[2], False)
            run_scala(sys.argv[1], sys.argv[2], False)

#        if len(sys.argv) > 3:
#            run_comp_cluster(sys.argv[1])
#        else:
        run_comp_local(sys.argv[1])
    except Exception as e:
        print(e)


