import http from "k6/http";
import { check, sleep } from "k6";

const baseUser = __ENV.USER_SERVICE_URL || "http://localhost:8001";
const basePost = __ENV.POST_SERVICE_URL || "http://localhost:8002";
const baseFeed = __ENV.FEED_SERVICE_URL || "http://localhost:8003";
const seed = JSON.parse(open("./seed-users.json"));
const users = seed.users;

export const options = {
  vus: Number(__ENV.K6_VUS || 50),
  duration: __ENV.K6_DURATION || "1m",
  thresholds: {
    http_req_failed: ["rate<0.008"],
    http_req_duration: ["p(95)<500"],
  },
};

function userId() {
  return users[Math.floor(Math.random() * users.length)];
}

export default function () {
  const roll = Math.random();
  if (roll < 0.7) {
    const res = http.get(`${baseFeed}/feeds/${userId()}?limit=50`);
    check(res, { "feed ok": (r) => r.status === 200 });
  } else if (roll < 0.9) {
    const author = userId();
    const res = http.post(
      `${basePost}/posts`,
      JSON.stringify({ author_id: author, content: `k6 post ${Date.now()} ${__VU}` }),
      { headers: { "content-type": "application/json" } }
    );
    check(res, { "post created": (r) => r.status === 201 });
  } else {
    const follower = userId();
    const followee = userId();
    if (follower !== followee) {
      const res = http.post(`${baseUser}/users/${follower}/follow/${followee}`);
      check(res, { "follow ok": (r) => r.status === 204 });
    }
  }
  sleep(0.1);
}

