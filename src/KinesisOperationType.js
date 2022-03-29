module.exports = {
  PUT: {
    getMaxCapacity: () => {
      return {
        type: 'PUT',
        BYTES: 1024 * 1024,
        COUNT: 1000,
      };
    },
    getMetricsToFetch: () => {
      return [
        'IncomingRecords',
        'IncomingBytes',
      ];
    },
  },
  GET: {
    getMaxCapacity: () => {
      return {
        type: 'GET',
        BYTES: 2 * 1024 * 1024,
        COUNT: 2000,
      };
    },
    getMetricsToFetch: () => {
      return [];
    },
  },
};