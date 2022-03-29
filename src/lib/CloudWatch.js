const AWS = require('aws-sdk');
const Utils = require('./Utils');

const sleepCap = 2000;
const tryCap = 20;

class CloudWatch {
  constructor(region) {
    this.cloudWatch = new AWS.CloudWatch({
      apiVersion: '2010-08-01',
      region,
    });
  }

  buildDimension(streamName, shardId) {
    return [
      {
        Name: 'StreamName',
        Value: streamName,
      }, {
        Name: 'ShardId',
        Value: shardId,
      },
    ];
  }

  buildQuery(id, metric, dimension, cloudWatchPeriod) {
    return {
      Id: `q${id}`,
      MetricStat: {
        Metric: {
          Dimensions: dimension,
          MetricName: metric.name,
          Namespace: 'AWS/Kinesis',
        },
        Period: cloudWatchPeriod,
        Stat: 'Sum',
        Unit: metric.unit,
      },
      ReturnData: true,
    };
  }

  async getMetricData(query, options) {
    let params = {
      MetricDataQueries: [
        query,
      ],
    };

    let res = await this.cloudWatch.getMetricData(
        Object.assign(params, options.params)).promise();

    res = res.MetricDataResults[0];
    return res.Values || [];
  }

  async getMaxShardRecord(streamName, shardId, options = {}) {
    let params = {
      MetricDataQueries: [
        {
          Id: 's1',
          MetricStat: {
            Metric: {
              Dimensions: this.buildDimension(streamName, shardId),
              MetricName: 'IncomingRecords',
              Namespace: 'AWS/Kinesis',
            },
            Period: 60,
            Stat: 'Sum',
            Unit: 'Count',
          },
          ReturnData: true,
        },
      ],
    };

    let res = await this.cloudwatch.getMetricData(
        Object.assign(params, options.params)).promise();

    res = res.MetricDataResults[0];
    res = res.Values || [];

    if (res.length < options.diffWindow) {
      for (let i = res.length; i < options.diffWindow; i++) {
        res.push(1000 * 60);
      }
    }

    return res.reduce((a, b) => {
      return Math.max(a, b);
    });
  }

  async getMetricStatistics(cwReq) {
    let cloudWatchMetrics;
    let tryCount = 1;
    let ok = false;

    do {
      try {
        cloudWatchMetrics = await this.cloudWatch.getMetricStatistics(cwReq).
            promise();

        ok = true;

      } catch (err) {
        console.log(err);

        tryCount++;
        if (tryCount >= tryCap) {
          throw err;
        }

        let sleepFor = Math.pow(2, tryCount * 100);

        await Utils.sleep(sleepFor > sleepCap ? sleepCap : sleepFor);
      }
    } while (!ok);

    return cloudWatchMetrics;
  }
}

module.exports = CloudWatch;