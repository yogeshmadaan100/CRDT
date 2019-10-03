# CRDT
Implementation of Last Write Wins (LWW) CRDT in Go

# Introduction
Conflict Free Replicated Data Types (CRDTs) are data structures that power real time collaborative applications in distributed systems. CRDTs can be replicated across systems, they can be updated independently and concurrently without coordination between the replicas, and it is always mathematically possible to resolve inconsistencies which might result.

# Recommended Reading
https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type

https://github.com/pfrazee/crdt_notes

https://hal.inria.fr/inria-00555588/PDF/techreport.pdf

# Running the program 
Just create a new main file and create LWW Set using `set1 := NewLWWSet()` .

# Running the tests
go test ./...
