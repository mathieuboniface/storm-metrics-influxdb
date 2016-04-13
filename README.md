# storm-metrics-influxdb
Apache Storm IMetricsConsumer implementation for influxdb (> 0.9)

### Installation
1. Clone this repo
1. Build the jar using maven : `mvn package`
1. Copy the built jar from target to storm lib directory : `cp target/storm-metrics-influxdb*.jar $STORM_INSTALL_DIR/lib/`
1. Restart your storm supervisors and workers

### Configuration (storm.yaml) 
```yaml
topology.metrics.consumer.register:
  - class: "fr.boniface.storm.metrics.influxdb.InfluxDBMetricsConsumer"
    parallelism.hint: 1
    argument:
      metrics.reporter.name: "fr.boniface.storm.metrics.influxdb.InfluxDBMetricsConsumer"
      metrics.influxdb.url: "http://HOSTNAME:PORT"
      metrics.influxdb.username: "USERNAME"
      metrics.influxdb.password: "PASSWORD"
      metrics.influxdb.db: "DATABASE"
      metrics.influxdb.measurement.prefix: "PREFIX"
```
