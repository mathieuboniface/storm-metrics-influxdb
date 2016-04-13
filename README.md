# storm-metrics-influxdb
Storm implementation of a metrics consumer for influxdb (> 0.9)

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
