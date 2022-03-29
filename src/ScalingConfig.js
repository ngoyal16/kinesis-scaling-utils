class ScalingConfig {
  #scaleThresholdPct;
  #scaleAfterMins;
  #coolOffMins;
  #scaleCount;
  #scalePct;

  constructor(config) {
    this.#scaleThresholdPct = config.scaleThresholdPct || 0;
    this.#scaleAfterMins = config.scaleAfterMins || 0;
    this.#coolOffMins = config.coolOffMins || 0;
    this.#scaleCount = config.scaleCount || 0;
    this.#scalePct = config.scalePct || 0;
  }

  getScaleThresholdPct() {
    return this.#scaleThresholdPct;
  }

  getScaleAfterMins() {
    return this.#scaleAfterMins;
  }

  getCoolOffMins() {
    return this.#coolOffMins;
  }

  setCoolOffMins(coolOffMins) {
    this.#coolOffMins = coolOffMins;
  }

  getScaleCount() {
    return this.#scaleCount;
  }

  getScalePct() {
    return this.#scalePct;
  }
}

module.exports = ScalingConfig;