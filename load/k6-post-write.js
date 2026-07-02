import http from "k6/http";
import { check, sleep } from "k6";

const basePost = __ENV.POST_SERVICE_URL || "http://localhost:8002";
const seed = JSON.parse(open("./seed-users.json"));
const users = seed.users;

export const options = {
  vus: Number(__ENV.K6_VUS || 25),
  duration: __ENV.K6_DURATION || "1m",
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<750"],
  },
};

function userId() {
  return users[Math.floor(Math.random() * users.length)];
}

export default function () {
  const res = http.post(
    `${basePost}/posts`,
    JSON.stringify({ author_id: userId(), content: `write load ${Date.now()} ${__VU}` }),
    { headers: { "content-type": "application/json" } }
  );
  check(res, { "post created": (r) => r.status === 201 });
  sleep(0.1);
}

