#### 1.0

* changed `mongo:connect` interfaces. Now options in connect/6 is proplist, where user can set Host and Port. 
In connect/1,3,5 default host and port are used.
* set default host:port as 127.0.0.1:27017. Is used, when no other specified.
* removed `auth` from mongo module. Made auth automatically in connect/3,5,6
* made `mc_worker:start_link` accept one parameter - a proplist with configuration.
* add ability for mc_worker to be a registered process if needed.


#### 2.0

* added SCRAM-SHA-1 auth support, used in mongodb 3+
* added server version request before each auth to determine the default auth mechanizm.