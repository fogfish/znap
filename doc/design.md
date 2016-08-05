# Design Considerations

The application is build using micro-kernel philosophy with small fail-safe kernel capable to correct failures anywhere. The kernel is surrounded by isolated subsystems. The error recovery responsibility is delegated to supervisors that guarantees application resilience.   

The application employs following structure

```
+- /user (root guardian process) 
   |
   +- /user/queues (ingress queue i/o signaling plane)
   |   |
   |   +- /user/queues/{host}:{port} (i/o signaling with queue instance )
   |   |  |
   |   |  +- ...
   |   |
   |   +- ...
   |
   +- /user/snapshots (last value caching, snapshot aggregation)
   |  |
   |  +- (tbd)
   |
   +- /user/httpd (rest interface)
      |
      +- ...
```
