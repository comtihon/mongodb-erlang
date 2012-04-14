# Design of the Erlang MongoDB driver
#### By Tony Hannan, June 2011

I am a 10gen employee an author of the official [Erlang MongoDB driver](http://github.com/mongo/mongodb-erlang). In Nov 2010, I was assigned the task of writing a production-quality Erlang driver. Today, I would say the official driver is production-quality. Below I highlight some design decisions. For detailed documentation with code examples please see the links at the end of this article.

### BSON

At the highest level, the driver is divided into two library applications, [mongodb](http://github.com/mongodb/mongodb-erlang) and [bson](http://github.com/mongodb/bson-erlang). Bson is defined independently of MongoDB at [bsonspec.org](http://bsonspec.org). One design decision was how to represent Bson documents in Erlang. Conceptually, a document is a record, but unlike an Erlang record, a Bson document does not have a single type tag. Futhermore, the same MongoDB collection can hold different types of records. So I decided to represent a Bson document as a tuple with labels interleaved with values, as in `{name, Name, address, Address}`. An alternative would have been to represent a document as a list of label-value pairs, but I wanted to reserve lists for Bson arrays.

A Bson value is one of several types. One of these types is the document type itself, making it recursive. Several value types are not primitive, like objectid and javascript, so I had to create a tagged tuple for each of them. I defined them all to have an odd number of elements to distinguish them from a document which has an even number of elements. Finally, to distinguish between a string and a list of integers, which is indistinguishable in Erlang, I require Bson strings to be binary (UTF-8). Therefore, a plain Erlang string is interpreted as a Bson array of integers, so make sure to always encode your strings, as in `<<"hello">>` or `bson:utf8("hello")`.

### Var

The mongodb driver has a couple of objects like connection and cursor that maintain mutable state. The only way to mutate state in Erlang is to have a process that maintains its own state and updates it when it receives messages from other processes. The Erlang programmer typically creates a process for each mutable object and defines the messages it may receive and the action to take for each message. He usually provides helper functions on top for the clients to call that hide the actual messages being sent and returned. Erlang OTP provides a small framework called *gen_server* to facilitate this process definition but it is still non-trivial. To alleviate this burden I created another abstraction on top of gen_server called *var*. A var is an object (process) that holds a value of some type A that may change. Its basic operation is `modify (var(A), fun ((A) -> {A, B})) -> B`, which applies the function to the current value of the var then changes the var's value to the first item of the result while returning the second item of the result to the client. This is done atomically thanks to the sequential nature of Erlang processes. The function may perform side effects (sending/receiving messages or doing IO), in which case the var acts like a mutex since only one function can execute against the var at a time. In essence, using var or even just gen_server changes the programming paradigm from message passing to protected shared state, which is more like Haskell for example.

With var it is now very easy to create objects that protect a shared resource or have internal mutable state. A TCP connection to a MongoDB server is one such resource that needs protection. The connection object in mongodb is implemented as a var holding a TCP connection. Every read and write operation gets exclusive access to the TCP connection when sending and receiving its messages to and from the server. This allows multiple user processes to read and write to the same mongodb connection concurrently.

### DB Action

Every read/write operation may throw a DB exception. Furthermore, every read/write operation requires a DB context that includes the connection to use, the database to access, whether slave is ok (read_mode), and whether to confirm writes (write_mode). We group a series of read/write operations that perform a single high-level task into a function called a *DB action*. We then execute the action within a single exception handler and with a single DB context in dynamic scope (using Erlang's process dictionary). `mongo:do (write_mode(), read_mode(), connection(), db(), action(A)) -> {ok, A} | {failure, failure()}` sets up the context, runs the action, and catches and returns any DB failure. Note, it will not catch and return other types of exceptions like programming errors.

You may notice that a DB action is analogous to a DB transaction for a relational database in that the action aborts when one of its operations fails. However, for scalability reasons, MongoDB does not support ACID across multiple read/write operations, so the operations before the failed operation remain in effect. Your failure handler must be prepared to recover from this intermediate state. If your DB action is conceptually a single high-level task, then it should not be to hard to undo and redo that task even from an intermediate state.

### Documentation

Detailed documentation with examples can be found in the ReadMe's of the two libraries, [mongodb](http://github.com/mongodb/mongodb-erlang) and [bson](http://github.com/mongodb/bson-erlang), and in their source code comments and test modules.

In addition recent docuemenation on the API will be avaliable on the [mongodb website](http://api.mongodb.org/erlang/mongodb/).

Should you wish to generate this from the latest code, you can run:

    $ cd docs
    $ sh ./gen_docs
