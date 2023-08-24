# KV Store based on Log-Structured File Technology

This project utilizes log-structured file technology and is developed using the Rust programming language. Currently, it is in a demonstrative phase.

## Core Logic

The essential functionality of this key-value (KV) database includes three core operations: get, set, and remove. In this system, set and remove operations are persisted into log-structured files. Simultaneously, the database maintains a mapping in memory from keys to the file pointers of the latest set transactions. When a get operation is performed, the system uses the file pointer associated with a specific key to read data from the file. Each log file has a fixed capacity, and when it reaches a predefined threshold, a new file is created to store new transactional data.

## Storage Structure

Below is an illustrative diagram of the storage structure:

![Storage Structure Diagram](./docs/kvs.svg)

Based on the aforementioned storage structure, the system's capacity can be easily calculated. Assuming an average key size of 100 bytes and a fixed size of 12 bytes for file pointers, each key would consume 112 bytes of memory.

Through this architecture, the system can efficiently manage key-value data and dynamically expand storage space as needed. This design allows the key-value store to handle substantial amounts of data effectively while maintaining performance. It's important to note that this is a high-level overview; the actual design and implementation of the system would involve various details and optimizations.