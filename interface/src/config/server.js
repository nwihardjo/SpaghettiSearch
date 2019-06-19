const PORT = process.env.PORT || 8080;
const HOST = '0.0.0.0' || 'localhost';
let address = "http://" + HOST + ":" + PORT + "/"

module.exports = {address: address};
