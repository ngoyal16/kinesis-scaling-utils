const AWS = require('aws-sdk');

const Utils = require('./Utils');

class Kinesis {
  constructor(region) {
    this.kinesis = new AWS.Kinesis({
      apiVersion: '2013-12-02',
      region,
    });
  }

  async describeStream(streamName) {
    const res = await this.kinesis.describeStreamSummary({
      StreamName: streamName,
    }).promise();

    return res['StreamDescriptionSummary'];
  }

  async getOpenShardCount(streamName) {
    return (await this.describeStream(streamName))['OpenShardCount'];
  }

  async getClosedShards(streamName, options) {
    const closeShards = [];

    const initialListShardsParams = {
      StreamName: streamName,
    };

    let hasMoreShards = true;
    let nextToken = null;

    while (hasMoreShards) {
      let shardsRes = await this.kinesis.listShards(Object.assign({
        MaxResults: 1000,
      }, options, nextToken ? {
        NextToken: nextToken,
      } : initialListShardsParams)).promise();

      for (let i = 0; i < shardsRes['Shards'].length; i++) {
        let shard = shardsRes['Shards'][i];
        // Take closed shards
        if ('EndingSequenceNumber' in shard.SequenceNumberRange) {
          closeShards.push(shard);
        }
      }

      if ('NextToken' in shardsRes) {
        nextToken = shardsRes['NextToken'];
      } else {
        hasMoreShards = false;
      }
    }

    // Sort shard in ASC order by hash key
    closeShards.sort(function(prev, curr) {
      let prevStartingHashKey = prev.HashKeyRange.StartingHashKey.padStart(40,
          '0');
      let currStartingHashKey = curr.HashKeyRange.StartingHashKey.padStart(40,
          '0');
      if (prevStartingHashKey < currStartingHashKey) {
        return -1;
      }

      if (prevStartingHashKey > currStartingHashKey) {
        return 1;
      }

      return 0;
    });

    return closeShards;
  }

  async getOpenShards(streamName) {
    const openShards = [];

    const initialListShardsParams = {
      StreamName: streamName,
    };

    let hasMoreShards = true;
    let nextToken = null;

    while (hasMoreShards) {
      let shardsRes = await this.kinesis.listShards(Object.assign({
        MaxResults: 1000,
      }, nextToken ? {
        NextToken: nextToken,
      } : initialListShardsParams)).promise();

      for (let i = 0; i < shardsRes['Shards'].length; i++) {
        let shard = shardsRes['Shards'][i];
        // Take open shards
        if (!('EndingSequenceNumber' in shard.SequenceNumberRange)) {
          openShards.push(shard);
        }
      }

      if ('NextToken' in shardsRes) {
        nextToken = shardsRes['NextToken'];
      } else {
        hasMoreShards = false;
      }
    }

    // Sort shard in ASC order by hash key
    openShards.sort(function(prev, curr) {
      let prevStartingHashKey = prev.HashKeyRange.StartingHashKey.padStart(40,
          '0');
      let currStartingHashKey = curr.HashKeyRange.StartingHashKey.padStart(40,
          '0');
      if (prevStartingHashKey < currStartingHashKey) {
        return -1;
      }

      if (prevStartingHashKey > currStartingHashKey) {
        return 1;
      }

      return 0;
    });

    return openShards;
  }

  async mergeShards(streamName, options) {
    let streamDetails;
    do {
      streamDetails = await this.describeStream(streamName);

      if (streamDetails.StreamStatus === 'ACTIVE') {
        break;
      }
    } while (true);

    let ok = false;
    let tryCount = 1;
    let sleepCap = 2000;
    let tryCap = 20;
    do {

      try {
        await this.kinesis.mergeShards({
          StreamName: streamName,
          ShardToMerge: options.ShardToMerge,
          AdjacentShardToMerge: options.AdjacentShardToMerge,
        }).promise();

        ok = true;
      } catch (err) {
        if (err.name === 'InvalidArgumentException') {
          ok = true;
        } else if (err.name === 'ResourceInUseException') {
          do {
            streamDetails = await this.describeStream(streamName);

            if (streamDetails.StreamStatus === 'ACTIVE') {
              break;
            }
          } while (true);
        } else {
          console.log(err);
        }
      }

      tryCount++;
      if (tryCount >= tryCap) {
        throw new Error('Active not being update after reaching try cap');
      }

      let sleepFor = Math.pow(2, tryCount * 100);

      await Utils.sleep(sleepFor > sleepCap ? sleepCap : sleepFor);
    } while (!ok);
  }

  async splitShard(streamName, options) {
    let streamDetails;
    do {
      streamDetails = await this.describeStream(streamName);

      if (streamDetails.StreamStatus === 'ACTIVE') {
        break;
      }
    } while (true);

    let ok = false;
    let tryCount = 1;
    let sleepCap = 2000;
    let tryCap = 20;
    do {

      try {
        await this.kinesis.splitShard({
          StreamName: streamName,
          ShardToSplit: options.ShardToSplit,
          NewStartingHashKey: options.NewStartingHashKey,
        }).promise();

        ok = true;
      } catch (err) {
        console.log(err);
        if (err.name === 'InvalidArgumentException') {
          ok = true;
        } else if (err.name === 'ResourceInUseException') {
          do {
            streamDetails = await this.describeStream(streamName);

            if (streamDetails.StreamStatus === 'ACTIVE') {
              break;
            }
          } while (true);
        } else {
          console.log(err);
        }
      }

      tryCount++;
      if (tryCount >= tryCap) {
        throw new Error('Active not being update after reaching try cap');
      }

      let sleepFor = Math.pow(2, tryCount * 100);

      await Utils.sleep(sleepFor > sleepCap ? sleepCap : sleepFor);
    } while (!ok);
  }
}

module.exports = Kinesis;