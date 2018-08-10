# Test1A

Two processes (plus group manager) required.

When the second process sends its fifth message this triggers
process P1 to send a multicast and then to crash. 

The multicast sent is programmed to be received by just one
other process other than the group manager. So the message
will be received and delivered just by P2.
When the group manager recognize P1 is crashed it starts
the view-change process.

P2 will then send the message P1 sent before to the group manager.

```
gradle run -Dconfig=<actor.conf> -Djson=test1a.json
```