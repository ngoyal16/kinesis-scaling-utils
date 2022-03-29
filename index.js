require('./src/lib/Array');

const AutoscalingConfiguration = require('./src/AutoscalingConfiguration');
const StreamMonitor = require('./src/StreamMonitor');
const config = require('./config.json');

console.log(config);

const autoscalingConfiguration = new AutoscalingConfiguration(config);
const streamMonitor = new StreamMonitor(autoscalingConfiguration);

(async () => {
  await streamMonitor.run();
})();