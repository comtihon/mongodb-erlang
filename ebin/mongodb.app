{application,mongodb,
             [{description,"Client interface to MongoDB, also known as the driver. See www.mongodb.org"},
              {vsn,"0.3.2"},
              {modules,[mongo,mongo_connect,mongo_cursor,mongo_protocol,
                        mongo_query,mongo_replset,mongodb_app,mongodb_tests,
                        mvar,resource_pool]},
              {registered,[]},
              {applications,[kernel,stdlib]},
              {mod,{mongodb_app,[]}}]}.
