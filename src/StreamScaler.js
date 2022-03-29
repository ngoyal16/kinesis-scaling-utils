class StreamScaler {
  #kinesisClient;

  constructor(kinesisClient) {
    this.#kinesisClient = kinesisClient;
  }
}

module.exports = StreamScaler;