const KinesisOperationType = require('./KinesisOperationType');

const CW_NAMESPACE = 'AWS/Kinesis';

class StreamMetricsManager {
  #cloudwatchRequestTemplates;

  constructor(
      streamName, cloudWatchPeriod, types, kinesisClient, cloudWatchClient) {
    this.streamName = streamName;
    this.cloudWatchPeriod = cloudWatchPeriod;
    this.trackedOperations = types;
    this.kinesisClient = kinesisClient;
    this.cloudWatchClient = cloudWatchClient;

    this.#cloudwatchRequestTemplates = {};

    for (let i = 0; i < this.trackedOperations.length; i++) {
      const kinesisOpts = KinesisOperationType[this.trackedOperations[i]];

      let metricsToFetch = kinesisOpts.getMetricsToFetch();
      for (let j = 0; j < metricsToFetch.length; j++) {
        const cwRequest = {
          Namespace: CW_NAMESPACE,
          Period: cloudWatchPeriod,
          Dimensions: [
            {
              Name: 'StreamName',
              Value: streamName,
            },
          ],
          Statistics: [
            'Sum',
          ],
          MetricName: metricsToFetch[j],
        };

        if (!(this.trackedOperations[i] in this.#cloudwatchRequestTemplates)) {
          this.#cloudwatchRequestTemplates[this.trackedOperations[i]] = [];
        }

        this.#cloudwatchRequestTemplates[this.trackedOperations[i]].push(
            cwRequest);
      }
    }
  }

  async loadMaxCapacity() {
    let openShards = await this.kinesisClient.getOpenShards(
        this.streamName);
  }

  async queryCurrentUtilisationMetrics(
      shardId, cwSampleDuration, metricStartTime, metricEndTime) {

    let currentUtilisationMetrics = {};

    for (let opsTypes in this.#cloudwatchRequestTemplates) {
      if (!(opsTypes in currentUtilisationMetrics)) {
        currentUtilisationMetrics[opsTypes] = {
          BYTES: {},
          COUNT: {},
        };
      }

      for (let i = 0; i <
      this.#cloudwatchRequestTemplates[opsTypes].length; i++) {

        let cwReq = JSON.parse(JSON.stringify(
            Object.assign({}, this.#cloudwatchRequestTemplates[opsTypes][i], {
              StartTime: metricStartTime,
              EndTime: metricEndTime,
            })));

        cwReq.Dimensions.push({
          Name: 'ShardId',
          Value: shardId,
        });

        let cloudWatchMetrics = await this.cloudWatchClient.getMetricStatistics(
            cwReq);

        for (let j = 0; j < cloudWatchMetrics.Datapoints.length; j++) {
          const metric = cloudWatchMetrics.Datapoints[j].Unit.toUpperCase();

          const metrics = currentUtilisationMetrics[opsTypes][metric];

          let sampleMetrics = 0;
          if (cloudWatchMetrics.Datapoints[j].Timestamp in metrics) {
            sampleMetrics = metrics[cloudWatchMetrics.Datapoints[j].Timestamp];
          }

          sampleMetrics += cloudWatchMetrics.Datapoints[j].Sum /
              this.cloudWatchPeriod;

          metrics[cloudWatchMetrics.Datapoints[j].Timestamp] = sampleMetrics;

          currentUtilisationMetrics[opsTypes][metric] = metrics;
        }

      }
    }

    return currentUtilisationMetrics;
  }
}

module.exports = StreamMetricsManager;