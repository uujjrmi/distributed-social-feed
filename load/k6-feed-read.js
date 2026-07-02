import http from "k6/http";
import { check, sleep } from "k6";

const baseFeed = __ENV.FEED_SERVICE_URL || "http://localhost:8003";
const seed = JSON.parse(open("./seed-users.json"));
const users = seed.users;

export const options = {
  vus: Number(__ENV.K6_VUS || 100),
  duration: __ENV.K6_DURATION || "2m",
  thresholds: {
    http_req_failed: ["rate<0.008"],
    http_req_duration: ["p(95)<500"],
  },
};

function userId() {
  return users[Math.floor(Math.random() * users.length)];
}

export default function () {
  const res = http.get(`${baseFeed}/feeds/${userId()}?limit=50`);
  check(res, { "feed ok": (r) => r.status === 200 });
  sleep(0.05);
}

