const http = require("http");

const data = JSON.stringify({
  eventId: "lab-level1-005",
  customerId: "customer-1",
  type: "ORDER_CREATED",
  amount: 100.50,
  source: "level1-test"
});

const req = http.request({
  hostname: "127.0.0.1",
  port: 8080,
  path: "/send-event",
  method: "POST",
  headers: {
    "Content-Type": "application/json",
    "Content-Length": Buffer.byteLength(data)
  }
}, res => {
  res.pipe(process.stdout);
});

req.on("error", error => {
  console.error(error);
  process.exit(1);
});

req.write(data);
req.end();
