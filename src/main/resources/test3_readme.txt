Required participants: 3 + group manager

In the first step p1, p2 and p3 send a multicast.
P3 crashes after sending the multicast, while p1 is
able to send only one message before crashing.

In step 2 p2 sends a multicast.

In step 3 p2 sends a multicast again and p1 revives as p4
In step 4 p4 (previously p1) sends a multicast and p3 revives
as p5.

In step 5 p4, p2 and p5 send a multicast.
