import http from "k6/http";
import { check } from "k6";
import { Rate } from "k6/metrics";

const baseURL = (__ENV.BASE_URL || "http://127.0.0.1:8080").replace(/\/$/, "");
const scene = __ENV.SCENE || "timeline";
const duration = __ENV.DURATION || "60s";
const successRate = new Rate("feed_cache_success_rate");

export const options = {
  scenarios: {
    hot_key: {
      executor: "constant-vus",
      vus: Number(__ENV.HOT_VUS || 40),
      duration,
      exec: "hotKey",
    },
    spread_keys: {
      executor: "constant-vus",
      vus: Number(__ENV.SPREAD_VUS || 10),
      duration,
      exec: "spreadKeys",
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
    "http_req_duration{scenario:hot_key}": ["p(95)<100"],
    "http_req_duration{scenario:spread_keys}": ["p(95)<300"],
    feed_cache_success_rate: ["rate>0.99"],
  },
};

function request(limit) {
  const response = http.get(`${baseURL}/api/feed-items?scene=${encodeURIComponent(scene)}&limit=${limit}`);
  const ok = check(response, {
    "status is 200": (result) => result.status === 200,
    "response has items": (result) => Array.isArray(result.json("items")),
  });
  successRate.add(ok);
}

export function setup() {
  request(10);
}

export function hotKey() {
  request(10);
}

export function spreadKeys() {
  request(1 + ((__VU + __ITER) % 50));
}
