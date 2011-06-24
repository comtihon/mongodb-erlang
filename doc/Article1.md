# Design of the Erlang MongoDB driver
#### By Tony Hannan, June 2011

I am a 10gen employee an author of the official [Erlang MongoDB driver](http://github.com/TonyGen/mongodb-erlang). In Nov 2010, I was assigned the task of ensuring a production quality Erlang driver. At that time, there were a couple of Erlang drivers in the community, [emongo](http://bitbucket.org/rumataestor/emongo) and [Erlmongo](http://github.com/wpntv/erlmongo). I was hoping to pick one and enhance it to become the official driver. However, after reading both of them, I felt neither were elegant enough for my taste. Please don't take this as an insult to those drivers. They are fine pieces of work. It is just that my tastes are very particular, and it was easier for me to start from scratch then to pick one and adapt to it or refactor it.

### BSON

At the highest level the official driver is divided into two library applications, [mongodb](http://github.com/TonyGen/mongodb-erlang) and [bson](http://github.com/TonyGen/bson-erlang). Bson is defined independently of MongoDB at [bsonspec.org](http://bsonspec.org). One design decision was how to represent Bson documents in Erlang. Conceptually, a document is a record, but unlike an Erlang record, a Bson document does not have a single type tag. Futhermore, the same MongoDB collection can hold different types of records. So I decided to represent a Bson document as a tuple with labels interspersed with values, as in `{name, Name, address, Address}`. An alternative would have been to represent a document as a list of label-value pairs, but I wanted to reserve lists for Bson arrays.

A Bson value is one of several types. One of these types is the document type itself, making it recursive. Several value types are not primitive, like objectid and javascript, so I had to create a tagged tuple (Erlang record type) for each of them. To distinguish them from a document which is also a tuple, I defined them all to have an odd number of elements. A tuple with an even number of elements is a document. Finally, to distinguish between a string and a list of integers, which is indistinguishable in Erlang, I require Bson strings to be binary (UTF-8). Therefore, a plain Erlang string is interpreted as a Bson array of integers, so make sure to always encode your strings, as in `<<"hello">>` or `bson:utf8("hello")`.

### Var

The mongodb driver has a couple of objects like connection and cursor that maintain mutable state. The only way to mutate state in Erlang is to have a process that maintains its own state and updates it when it receives messages from other processes. The Erlang programmer typically creates a process for each mutable object and defines the messages it may receive and the action to take for each message. He usually provides helper functions on top for the clients to call that hide the actual messages being sent and returned. Erlang OTP provides a small framework called *gen_server* to facilitate this process definition but it is still non-trivial. To alleviate this burden I created another abstraction on top of gen_server called *var*. A var is an object (process) that holds a value of some type A that may change. Its basic operation is `modify (var(A), fun ((A) -> {A, B})) -> B`, which applies the function to the current value of the var then changes the var's value to the first item of the result while returning the second item of the result to the client. This is done atomically thanks to the sequential nature of Erlang processes. The function may perform side effects (sending/receiving messages or doing IO), in which case the var acts like a mutex since only one function can execute against the var at a time. In essence, using var or even just gen_server changes the programming paradigm from message passing to protected shared state, which is more like Haskell for example.

With var it is now very easy to create objects that protect a shared resource or have internal mutable state. A TCP connection to a MongoDB server is one such resource that needs protection. The connection object in the mongodb driver is implemented as a var holding a TCP connection. Every read and write operation gets exclusive access to the TCP connection when sending and receiving its messages to and from the server. This allows multiple user processes to read and write on the same mongodb connection object concurrently.

### DB Action

Every read/write operation may throw an exception, furthermore, every read/write operation requires a context that includes the connection to use, the database to access, whether slave is ok, and whether to confirm writes. The user catches exceptions of and provides context to a block of code called a *DB action*. Conceptually, a DB action performs a high-level task that involves a series of read/write operations. The read/write operations in the task (action) use the same context and exception handler.

### Documentation

Detailed documentation can be found in the ReadMe's of the two libraries, [mongodb](http://github.com/TonyGen/mongodb-erlang#readme) and [bson](http://github.com/TonyGen/bson-erlang#readme), and in the source code comments.


