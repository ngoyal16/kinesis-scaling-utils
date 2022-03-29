const BigInteger = require('node-biginteger');
const CloudWatch = require('./lib/CloudWatch');
const Kinesis = require('./lib/Kinesis');
const Utils = require('./lib/Utils');

const KinesisOperationType = require('./KinesisOperationType');
const StreamMetricsManager = require('./StreamMetricsManager');
const StreamScaler = require('./StreamScaler');
const StreamScalingUtils = require('./StreamScalingUtils');

const CLOUDWATCH_PERIOD = 60;

class StreamMonitor {
  #config;
  #scaler;
  #keepRunning = true;
  #kinesisClient;
  #cloudWatchClient;

  constructor(config) {
    this.#config = config;

    this.#kinesisClient = new Kinesis(config.getRegion());
    this.#cloudWatchClient = new CloudWatch(config.getRegion());

    this.#scaler = new StreamScaler(this.#kinesisClient);
  }

  async run() {
    let lastShardCapacityRefreshTime = new Date();

    const metricManager = new StreamMetricsManager(
        this.#config.getStreamName(), CLOUDWATCH_PERIOD,
        this.#config.getScaleOnOperations(),
        this.#kinesisClient, this.#cloudWatchClient);

    const cwSampleDuration = Math.max(
        this.#config.getScaleUp().getScaleAfterMins(),
        this.#config.getScaleDown().getScaleAfterMins());

    do {
      let closedShards;
      do {
        closedShards = await this.#kinesisClient.getClosedShards(
            this.#config.getStreamName(), {
              ShardFilter: {
                Timestamp: Utils.minusMinutes(new Date(), cwSampleDuration + 1),
                Type: 'FROM_TIMESTAMP',
              },
            });

        await Utils.sleep(cwSampleDuration * 1000);
      } while (closedShards.length);

      const now = new Date();
      const metricStartTime = Utils.minusMinutes(now, cwSampleDuration);

      let openShards = await this.#kinesisClient.getOpenShards(
          this.#config.getStreamName());

      let numOpenShards = openShards.length;

      let canMergeShards = [];
      for (let i = 0; i < openShards.length; i++) {
        let currentUtilisationMetrics = await metricManager.queryCurrentUtilisationMetrics(
            openShards[i].ShardId, cwSampleDuration, metricStartTime, now);

        openShards[i].action = await this.processCloudwatchMetrics(
            currentUtilisationMetrics,
            cwSampleDuration);

        console.log(openShards[i].action);

        if (openShards[i].action.scaleDirection === 'UP') {
          await this.#kinesisClient.splitShard(this.#config.getStreamName(), {
            ShardToSplit: openShards[i].ShardId,
            NewStartingHashKey: BigInteger.fromString(
                openShards[i].HashKeyRange.StartingHashKey).
                add(BigInteger.fromString(
                    openShards[i].HashKeyRange.EndingHashKey)).
                divide(BigInteger.fromString('2')).
                toString(),
          });

          numOpenShards++;
        } else {
          if (i !== 0) {
            if (
                openShards[i - 1].action.scaleDirection === 'DOWN' &&
                openShards[i].action.scaleDirection === 'DOWN'
            ) {
              canMergeShards.push({
                maxScaleValue: (openShards[i - 1].action.maxScaleValue +
                    openShards[i].action.maxScaleValue) / 2,
                ShardToMerge: openShards[i - 1].ShardId,
                AdjacentShardToMerge: openShards[i].ShardId,
              });
            }
          }
        }
      }

      if (canMergeShards.length) {
        let newShardCount = StreamScalingUtils.getNewShardCount(numOpenShards,
            this.#config.getScaleDown().getScaleCount(),
            this.#config.getScaleDown().getScalePct(), 'DOWN',
            this.#config.getMinShards(), this.#config.getMaxShards());

        if (numOpenShards > newShardCount) {
          canMergeShards.sortByProp('maxScaleValue');

          while ((numOpenShards > newShardCount) &&
          canMergeShards.length) {
            try {
              let mergeShard = canMergeShards.shift();

              console.log(mergeShard);

              await this.#kinesisClient.mergeShards(
                  this.#config.getStreamName(),
                  mergeShard,
              );

              canMergeShards = canMergeShards.filter(item => {
                return item.AdjacentShardToMerge !== mergeShard.ShardToMerge &&
                    item.ShardToMerge !== mergeShard.AdjacentShardToMerge;
              });

              numOpenShards--;
            } catch (err) {
              console.log(err);
            } finally {
              console.log(numOpenShards, newShardCount,
                  canMergeShards.length);
            }
          }
        }
      }

      await Utils.sleep(this.#config.getCheckInterval() * 1000);
    } while (this.#keepRunning);
  }

  async processCloudwatchMetrics(currentUtilisationMetrics, cwSampleDuration) {
    let scaleVotes = {},
        finalScaleDirection,
        finalMaxScaleValue = 0;

    for (let opsTypes in currentUtilisationMetrics) {
      scaleVotes[opsTypes] = 'NONE';

      let perMetricSamples = {
        'COUNT': [0, 0, 0],
        'BYTES': [0, 0, 0],
      };

      for (let streamMetric in currentUtilisationMetrics[opsTypes]) {
        let latestAvg = 0,
            latestPct = 0,
            latestMax = 0,
            lowSamples = 0,
            highSamples = 0,
            streamMax = 0,
            currentMax = 0,
            currentPct = 0,
            lastTime = null;

        let metrics = currentUtilisationMetrics[opsTypes][streamMetric];

        if (Object.keys(metrics).length === 0) {
          lowSamples = this.#config.getScaleDown().getScaleAfterMins();
        }

        for (let datapointEntry in metrics) {
          currentMax = metrics[datapointEntry];
          streamMax = (KinesisOperationType[opsTypes].getMaxCapacity())[streamMetric];
          currentPct = currentMax / streamMax;

          if (lastTime == null || new Date(datapointEntry) > lastTime) {
            latestPct = currentPct;
            latestMax = currentMax;

            // latest average is a simple moving average
            latestAvg = latestAvg === 0 ?
                currentPct : (latestAvg + currentPct) / 2;
          }

          lastTime = datapointEntry;

          // if the pct for the datapoint exceeds the configured threshold, then count a
          // high sample, otherwise it's a low sample
          if (currentPct > this.#config.getScaleUp().getScaleThresholdPct() /
              100) {
            highSamples++;
          } else if (currentPct <
              this.#config.getScaleDown().getScaleThresholdPct() / 100) {
            lowSamples++;
          }
        }

        if (Object.keys(metrics).length < cwSampleDuration) {
          lowSamples += cwSampleDuration - Object.keys(metrics).length;
        }

        perMetricSamples[streamMetric] = [highSamples, lowSamples, latestAvg];
      }

      /*-
     * we now have per metric samples for this operation type
     *
     * For Example:
     *
     * Metric  | High Samples | Low Samples | Pct Used
     * Bytes   | 3            | 0           | .98
     * Records | 0            | 10          | .2
     *
     * Check these values against the provided configuration. If we have
     * been above the 'scaleAfterMins' with high samples for either
     * metric, then we scale up. If not, then if we've been below the
     * scaleAfterMins with low samples, then we scale down. Otherwise
     * the vote stays as NONE
     */

      // first find out which of the dimensions of stream utilisation are
      // higher - we'll use the higher of the two for time checks
      let higherUtilisationMetric = 'COUNT',
          higherUtilisationPct = perMetricSamples.COUNT[2];

      if (perMetricSamples.BYTES[2] > perMetricSamples.COUNT[2]) {
        higherUtilisationMetric = 'BYTES';
        higherUtilisationPct = perMetricSamples.BYTES[2];
      }

      finalMaxScaleValue = Math.max(finalMaxScaleValue, higherUtilisationPct);

      if (perMetricSamples[higherUtilisationMetric][0] >=
          this.#config.getScaleUp().getScaleAfterMins()) {

        scaleVotes[opsTypes] = 'UP';
      } else if (perMetricSamples[higherUtilisationMetric][1] >=
          this.#config.getScaleDown().getScaleAfterMins()) {
        scaleVotes[opsTypes] = 'DOWN';
      }
    }

    const getVote = scaleVotes['GET'];
    const putVote = scaleVotes['PUT'];
    if (getVote && putVote) {
      if (getVote === 'UP' || putVote === 'UP') {
        finalScaleDirection = 'UP';
      } else if (getVote === 'DOWN' && putVote === 'DOWN') {
        finalScaleDirection = 'DOWN';
      } else {
        finalScaleDirection = 'NONE';
      }
    } else {
      finalScaleDirection = getVote ? getVote : putVote;
    }

    return {
      scaleDirection: finalScaleDirection,
      maxScaleValue: finalMaxScaleValue,
    };
  }

  async stop() {
    this.#keepRunning = false;
  }
}

module.exports = StreamMonitor;