#  ALLEN DANIEL
#  YESA
#  ALLENDAN

# Connected Components

from pyspark import SparkConf, SparkContext
import sys

#function to emit neighbours from the input file :: Emits (u,v) and (v,u) where input is (u,v)
def emit_neigh(line):
	l = line.split(" ")
	v = [ int(x) for x in l ]
	return [(v[0], v[1]), (v[1], v[0])]

#function to emit the minimum of key and value along with key :: Emits (u,min(u,v)) where input is (u,v)
def min_keyvalue(x): 
	v1 = [ int(y) for y in x ]
	return (v1[0], min(v1[0],v1[1]))

#function to emit neighbour and minimum for given vertex :: Emits (v,m) if (u<v) where input is (u,(v,m))
def large_emit(z):
	if (int(z[0])<int(z[1][0])):
		return (int(z[1][0]),int(z[1][1]))

#first emit in small star to get the larger vertex and smaller neighbout :: Emits (u,v) if (v<=u) else emits (v,u)
def smallemit(s):
	v3 = [ int(y) for y in s ]
	if(v3[1]<=v3[0]):
		return (v3[0],v3[1])
	else:
		return (v3[1],v3[0])

#emit neighbour and minimum if neighbour!=minimum else emit vertex and neighbour as it is :: Emits (v,m) if (v!=m) else emits (u,v) where input is (u,(v,m)) 
def neighmin(y):
	if(int(y[1][0])!=int(y[1][1])):
		return (int(y[1][0]),int(y[1][1]))
	else:
		return (int(y[0]),int(y[1][0]))

#function to emit both pairs :: Emits (u,v) and (v,u) where input is (u,v)
def emit_both(l):
	v7 = [ int(x) for x in l ]
	return [(v7[0], v7[1]), (v7[1], v7[0])]

#function to get the desired output format without comma
def eliminating_comma(x):
	v8 = [int(a) for a in x ]
	return [(str(v8[0])+" "+str(v8[1])),(str(v8[1])+" "+str(v8[1]))]

if __name__ == "__main__":
	conf = SparkConf().setAppName("Connected_Components")
	sc = SparkContext(conf = conf)
	lines = sc.textFile(sys.argv[1])
	neigh = lines.flatMap(emit_neigh)
	neigh.persist()
	while(1):
		minkeyval_ls = neigh.reduceByKey(lambda a, b: min(a,b)).map(min_keyvalue) #this RDD contains minimum neighbour for each vertex incuding itself for large star
 		l_star_emit = neigh.join(minkeyval_ls).map(large_emit).filter(lambda a: a is not None) #output of large star
		s = l_star_emit.map(smallemit)	
		minkeyval_ss = s.reduceByKey(lambda a, b: min(a,b)).map(min_keyvalue) #this RDD contains minimum neighbour for each vertex incuding itself for small star
		s_star_emit = s.join(minkeyval_ss).map(neighmin) #output of small star
		if(s_star_emit.flatMap(emit_both).subtract(neigh).isEmpty() and neigh.subtract(s_star_emit.flatMap(emit_both)).isEmpty()):#condition to converge
			break
		else:
			neigh = s_star_emit.flatMap(emit_both)
			neigh.persist()
	output_rdd=s_star_emit.flatMap(eliminating_comma).distinct()
	output_rdd.saveAsTextFile(sys.argv[2])
	sc.stop()
