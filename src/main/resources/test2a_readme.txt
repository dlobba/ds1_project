# Test2A

Two processes (plus group manager) required.

When the second process starts it will be set to crash on the next
view-change message.

When sending its fifth message P2 triggers
process P1 to crash. The view change will then be initiated
and P0 (the group manager) will start sending view-change message.

When P2 receives the message it crashes.

```
gradle run -Dconfig=<actor.conf> -Djson=test2a.json
```