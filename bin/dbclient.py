#! /usr/bin/env python
import zmq
context=zmq.Context()
print "Connecting-to-confdb"
socket=context.socket(zmq.REQ)
socket.connect("tcp://localhost:4050")
for request in range(10):
    print("Sending request",request,"..")
    socket.send("Hello")
    message=socket.recv()
    print("Received reply",request,"[",message,"]")

