module.exports = {
  minusMinutes: (date, mins) => {
    date = new Date(date);
    date.setMinutes(date.getMinutes() - mins);

    return date;
  },
  sleep: (ms) => {
    return new Promise((resolve) => {
      setTimeout(resolve, ms);
    });
  },
};