const KinesisOperationType = require('./KinesisOperationType');
const ScalingConfig = require('./ScalingConfig');

class AutoscalingConfiguration {
  #streamName;
  #scaleUp;
  #scaleDown;
  #minShards;
  #maxShards;
  #scaleOnOperations;
  #checkInterval;
  #awsRegion;

  constructor(config = {}) {

    this.#awsRegion = config.region;

    this.#streamName = config.streamName;
    this.#minShards = config.minShards;
    this.#maxShards = config.maxShards;
    this.#checkInterval = config.checkInterval || 0;
    this.#scaleOnOperations = config.scaleOnOperation || [];

    this.#scaleUp = new ScalingConfig(config.scaleUp || {});
    this.#scaleDown = new ScalingConfig(config.scaleDown || {});

    this.validate();
  }

  getRegion() {
    return this.#awsRegion;
  }

  getStreamName() {
    return this.#streamName;
  }

  getScaleUp() {
    return this.#scaleUp;
  }

  getScaleDown() {
    return this.#scaleDown;
  }

  getCheckInterval() {
    return this.#checkInterval;
  }

  getScaleOnOperations() {
    return this.#scaleOnOperations;
  }

  getMinShards() {
    return this.#minShards;
  }

  getMaxShards() {
    return this.#maxShards;
  }

  validate() {
    if (this.#streamName == null || this.#streamName === '') {
      throw new Error('Stream Name must be specified');
    }

    if (this.#scaleUp == null && this.#scaleDown == null) {
      throw new Error(
          'Must provide at least one scale up or scale down configuration');
    }

    if ((this.#scaleUp != null && this.#scaleUp.getScalePct() == null) ||
        this.#scaleUp.getScalePct() < 0) {
      throw new Error(
          `Scale Up Percentage of ${this.#scaleUp.getScalePct()} is invalid or null`);
    }

    if ((this.#scaleDown != null && this.#scaleDown.getScalePct() == null) ||
        this.#scaleDown.getScalePct() < 0) {
      throw new Error(
          `Scale Down Percentage of ${this.#scaleDown.getScalePct()} is invalid or null`);
    }

    if (this.#minShards != null && this.#maxShards != null && this.#minShards >
        this.#maxShards) {
      throw new Error('Min Shard Count must be less than Max Shard Count');
    }

    // set the default operation types to 'all' if none were provided
    if (this.#scaleOnOperations == null || this.#scaleOnOperations.length ===
        0) {
      this.#scaleOnOperations = Object.keys(KinesisOperationType);
    }

    // set a 0 cool off if none was provided
    if (this.#scaleDown.getCoolOffMins() == null) {
      this.#scaleDown.setCoolOffMins(0);
    }

    if (this.#scaleUp.getCoolOffMins() == null) {
      this.#scaleUp.setCoolOffMins(0);
    }
  }
}

module.exports = AutoscalingConfiguration;