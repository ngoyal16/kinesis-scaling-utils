module.exports = {
  getNewShardCount: (
      currentShardCount, scaleCount, scalePct, scaleDirection, minShardsAllowed,
      maxShardsAllowed) => {

    let newShardCount = 0;

    if (scaleDirection === 'UP') {
      if (scaleCount) {
        newShardCount = currentShardCount + scaleCount;
      } else {
        // convert the scaling factor to a % above 100 if below - many customers use
        // values above or below as the config value is an int
        let scalingFactor = 0;
        if (scalePct < 100) {
          scalingFactor = (100 + scalePct) / 100;
        } else {
          scalingFactor = scalePct / 100;
        }

        newShardCount = Math.ceil(currentShardCount * scalingFactor);
      }

      if (maxShardsAllowed && newShardCount > maxShardsAllowed) {
        newShardCount = maxShardsAllowed;
      }
    } else {
      if (scaleCount) {
        newShardCount = currentShardCount - scaleCount;
      } else {
        // convert the scaling factor to a % above 100 if below - many customers use
        // values above or below as the config value is an int

        let scalingFactor = scalePct / 100;
        if (scalePct > 100) {
          newShardCount = Math.floor(currentShardCount / scalingFactor);
        } else {
          newShardCount = currentShardCount -
              Math.floor(currentShardCount * scalingFactor);
        }
      }

      // guard against going below min
      if (minShardsAllowed && newShardCount < minShardsAllowed) {
        newShardCount = minShardsAllowed;
      } else {
        // add a guard against any decision going below 1
        if (newShardCount < 1) {
          newShardCount = 0;
        }
      }
    }

    return newShardCount;
  },
};