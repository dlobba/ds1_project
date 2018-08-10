# Test3A

Two processes (plus group manager) required.

When the second process is sending its fifth message this triggers
process P1 to crash on the next data message receiving.

```
gradle run -Dconfig=<actor.conf> -Djson=test3a.json
```