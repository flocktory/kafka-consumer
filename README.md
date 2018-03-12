[![Clojars Project](https://clojars.org/com.flocktory/kafka-consumer/latest-version.svg)](https://clojars.org/com.flocktory/kafka-consumer)

[change log](CHANGELOG.md) | [getting started](https://github.com/flocktory/kafka-consumer/wiki/Getting-started) | [documentation](https://github.com/flocktory/kafka-consumer/wiki)

# Why?
In (mostly) every kafka consumer we use in our pipeline, the same guarantees are needed: 
do not move forward until record is successfully consumed (handled without exception), infinitely
retry to process failed records and automatically recover. In consequence, we want to manage and
commit offsets manually. That's why this abstraction over java consumer API was written.

# Features
- [support for different consume protocols](https://github.com/flocktory/kafka-consumer/wiki/Consumer-protocols)
- [safe consume, retry if exception is thrown](https://github.com/flocktory/kafka-consumer/wiki/Pause-resume-mechanism)
- no need to manage offsets manually
- [pluggable tracers for logs and metrics](https://github.com/flocktory/kafka-consumer/wiki/Logs-and-metrics)

# Usage

Add dependency to `project.clj`:

```clojure
[com.flocktory/kafka-consumer "0.0.2"]
```

See [getting started](https://github.com/flocktory/kafka-consumer/wiki/Getting-started) for more info.
