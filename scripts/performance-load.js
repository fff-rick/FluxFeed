import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";

const baseURL = (__ENV.BASE_URL || "http://127.0.0.1:8080").replace(/\/$/, "");
const prometheusURL = (__ENV.PROMETHEUS_URL || "http://127.0.0.1:9090").replace(/\/$/, "");
const profile = __ENV.PROFILE || "smoke";
const limit = positiveNumber("LIMIT", 20);
const runID = __ENV.RUN_ID || `${Date.now()}`;
const videoIDs = idList("VIDEO_IDS");
const targetUserIDs = idList("TARGET_USER_IDS");

const businessSuccess = new Rate("business_success_rate");
const unexpectedFailure = new Rate("unexpected_failure_rate");
const rateLimited = new Counter("rate_limited");
const operationDuration = new Trend("operation_duration", true);
const recommendationDegradation = new Counter("recommendation_degradation");

const recommendationGovernanceDecisions = ["fallback", "circuit_open", "bulkhead_full", "partial_recall"];
const recommendationProfiles = ["smoke", "baseline-recommend", "mixed", "capacity", "spike", "soak"];
const mixedProfiles = ["mixed", "capacity", "spike", "soak"];

const defaultAccounts = parseDefaultAccounts();
const readAccounts = parseAccountPool("READ_ACCOUNTS_JSON", defaultAccounts);
const writeAccounts = parseAccountPool("WRITE_ACCOUNTS_JSON", defaultAccounts);
const profiles = {
  smoke: {
    scenarios: {
      smoke: { executor: "constant-vus", vus: 1, duration: env("DURATION", "1m"), exec: "smoke" },
    },
  },
  "baseline-timeline": baselineScenario("timeline"),
  "baseline-hot": baselineScenario("hot"),
  "baseline-recommend": baselineScenario("recommend"),
  write: {
    scenarios: {
      publish: arrival("publish", positiveNumber("PUBLISH_RPS", 2), env("DURATION", "10m"), 10),
      like: arrival("like", positiveNumber("LIKE_RPS", 10), env("DURATION", "10m"), 30),
      follow: arrival("follow", positiveNumber("FOLLOW_RPS", 5), env("DURATION", "10m"), 20),
    },
  },
  mixed: mixedScenarios(env("DURATION", "15m")),
  capacity: {
    scenarios: scheduledSteps("capacity", [25, 50, 100, 200], env("STAGE_DURATION", "3m"), "weightedTraffic"),
  },
  spike: {
    scenarios: {
      before_spike: scheduledArrival("weightedTraffic", 20, "2m", "0s", 60),
      spike: scheduledArrival("weightedTraffic", 200, "1m", "2m", 500),
      after_spike: scheduledArrival("weightedTraffic", 20, "3m", "3m", 60),
    },
  },
  soak: {
    scenarios: {
      soak: arrival("weightedTraffic", positiveNumber("RPS", 50), env("DURATION", "30m"), 150),
    },
  },
  governance: {
    scenarios: scheduledSteps("governance", [60, 100, 200], env("STAGE_DURATION", "2m"), "governance"),
  },
};

if (!profiles[profile]) {
  throw new Error(`unsupported PROFILE=${profile}; choose ${Object.keys(profiles).join(", ")}`);
}

export const options = {
  ...profiles[profile],
  thresholds: thresholds(profile),
  discardResponseBodies: false,
  setupTimeout: "5m",
  summaryTrendStats: ["min", "med", "avg", "p(95)", "p(99)", "max"],
};

let cursors = {};

export function setup() {
  const needsAuth = profile !== "baseline-timeline" && profile !== "baseline-hot" && profile !== "governance";
  if (!needsAuth) {
    const governanceBaseline = monitorsRecommendation() ? recommendationGovernanceCounters() : {};
    return { readUsers: [], writeUsers: [], governanceBaseline };
  }

  const needsReadPool = profile !== "write";
  const needsWritePool = profile === "smoke" || profile === "write" || mixedProfiles.includes(profile);
  if (needsReadPool && readAccounts.length === 0) {
    throw new Error(`${profile} requires READ_ACCOUNTS_JSON or ACCOUNT/PASSWORD`);
  }
  if (needsWritePool && writeAccounts.length === 0) {
    throw new Error(`${profile} requires WRITE_ACCOUNTS_JSON or ACCOUNT/PASSWORD`);
  }
  if (mixedProfiles.includes(profile)) assertDisjointPools(readAccounts, writeAccounts);
  if (["smoke", "write", "mixed", "capacity", "spike", "soak"].includes(profile)) {
    if (videoIDs.length === 0) throw new Error(`${profile} requires VIDEO_IDS`);
    if (targetUserIDs.length === 0) throw new Error(`${profile} requires TARGET_USER_IDS`);
  }

  const governanceBaseline = monitorsRecommendation() ? recommendationGovernanceCounters() : {};
  const readUsers = needsReadPool ? loginAccounts(readAccounts) : [];
  const writeUsers = needsWritePool ? loginAccounts(writeAccounts) : [];
  return { readUsers, writeUsers, governanceBaseline };
}

function loginAccounts(accounts) {
  return accounts.map((account) => {
    const login = http.post(
      `${baseURL}/api/sessions`,
      JSON.stringify({ account: account.account, password: account.password }),
      jsonParams("setup_login"),
    );
    if (login.status !== 200) {
      throw new Error(`login failed for ${account.account}: status=${login.status} body=${login.body}`);
    }
    const token = safeJSON(login).access_token;
    if (!token) throw new Error(`login response has no access_token for ${account.account}`);

    const me = http.get(`${baseURL}/api/users/me`, authParams(token, "setup_me"));
    const body = safeJSON(me);
    if (me.status !== 200 || !Number.isInteger(body.id)) {
      throw new Error(`profile lookup failed for ${account.account}: status=${me.status} body=${me.body}`);
    }
    return { token, id: body.id, account: account.account };
  });
}

export function teardown(data) {
  if (!monitorsRecommendation()) return;

  // Prometheus currently scrapes every 15 seconds; wait for the final application counters.
  sleep(16);
  const current = recommendationGovernanceCounters();
  for (const decision of recommendationGovernanceDecisions) {
    const before = Number(data.governanceBaseline[decision] || 0);
    const after = Number(current[decision] || 0);
    if (after < before) {
      throw new Error(`recommendation governance counter reset during test: ${decision} ${before} -> ${after}`);
    }
    const delta = after - before;
    recommendationDegradation.add(delta, { decision });
    check(delta, { [`recommendation ${decision} delta is 0`]: (value) => value === 0 });
    console.log(`recommendation governance ${decision}: before=${before} after=${after} delta=${delta}`);
  }
}

export function smoke(data) {
  timeline(data);
  hot(data);
  recommend(data);
  if (videoIDs.length > 0) {
    exposure(data);
    like(data);
  }
  if (targetUserIDs.length > 0) follow(data);
  publish(data);
  sleep(1);
}

export function timeline(data) {
  feedGET("timeline", userFor(data, "readUsers"));
}

export function hot(data) {
  feedGET("hot", userFor(data, "readUsers"));
}

export function recommend(data) {
  const user = requireUser(data, "recommend", "readUsers");
  const cursor = nextCursor("recommend");
  const response = http.post(
    `${baseURL}/api/feed-queries`,
    JSON.stringify({
      scene: "recommend",
      cursor,
      limit,
      context: { request_id: requestID("recommend") },
    }),
    authParams(user.token, "recommend", { scene: "recommend", page: cursor ? "next" : "first" }),
  );
  record(response, "recommend", 200, (body) => feedBodyIsValid(body, "recommend"));
  updateCursor("recommend", response);
}

export function exposure(data) {
  const user = requireUser(data, "exposure", "writeUsers");
  const videoID = requireTarget(videoIDs, "VIDEO_IDS");
  const response = http.post(
    `${baseURL}/api/video-view-events`,
    JSON.stringify({
      video_id: videoID,
      scene: "recommend",
      request_id: requestID("exposure"),
      event_type: "exposed",
      watch_ms: 0,
      completed: false,
    }),
    authParams(user.token, "exposure"),
  );
  record(response, "exposure", 201, (body) => body.event && body.event.video_id === videoID);
}

export function like(data) {
  const user = requireUser(data, "like", "writeUsers");
  const videoID = requireTarget(videoIDs, "VIDEO_IDS");
  const response = http.put(
    `${baseURL}/api/videos/${videoID}/like`,
    null,
    authParams(user.token, "like", {}, true),
  );
  record(response, "like", 200, (body) => body.video_id === videoID && body.action_type === "LIKE" && body.active === true);
}

export function follow(data) {
  const user = requireUser(data, "follow", "writeUsers");
  const targetID = requireTarget(targetUserIDs.filter((id) => id !== user.id), "TARGET_USER_IDS");
  const response = http.put(
    `${baseURL}/api/users/me/following/${targetID}`,
    null,
    authParams(user.token, "follow", {}, true),
  );
  record(response, "follow", 200, (body) => body.target_user_id === targetID && body.following === true);
}

export function publish(data) {
  const user = requireUser(data, "publish", "writeUsers");
  const response = http.post(
    `${baseURL}/api/videos`,
    JSON.stringify({
      title: `perf-${runID}-${__VU}-${__ITER}`,
      description: "FluxFeed performance test",
      media_url: env("MEDIA_URL", "/uploads/performance.mp4"),
      cover_url: env("COVER_URL", "/uploads/performance.webp"),
    }),
    authParams(user.token, "publish", {}, true),
  );
  record(response, "publish", 201, (body) => Number.isInteger(body.id) && body.id > 0 && body.author_id === user.id);
}

export function weightedTraffic(data) {
  const bucket = (__VU * 31 + __ITER * 17) % 100;
  if (bucket < 45) return timeline(data);
  if (bucket < 60) return hot(data);
  if (bucket < 80) return recommend(data);
  if (bucket < 90) return exposure(data);
  if (bucket < 95) return like(data);
  if (bucket < 98) return follow(data);
  return publish(data);
}

export function governance() {
  const response = http.get(
    `${baseURL}/api/feed-items?scene=timeline&limit=${limit}`,
    jsonParams("governance", { scene: "timeline" }),
  );
  const expected = response.status === 200 || response.status === 429;
  check(response, { "governance returns 200 or 429": () => expected });
  businessSuccess.add(expected, { operation: "governance" });
  unexpectedFailure.add(!expected, { operation: "governance" });
  operationDuration.add(response.timings.duration, { operation: "governance" });
  if (response.status === 429) {
    rateLimited.add(1);
    check(response, { "429 has Retry-After 1": (result) => result.headers["Retry-After"] === "1" });
  }
}

function feedGET(scene, user) {
  const cursor = nextCursor(scene);
  const query = [`scene=${encodeURIComponent(scene)}`, `limit=${limit}`];
  if (cursor) query.push(`cursor=${encodeURIComponent(cursor)}`);
  const params = user
    ? authParams(user.token, scene, { scene, page: cursor ? "next" : "first" })
    : jsonParams(scene, { scene, page: cursor ? "next" : "first" });
  const response = http.get(`${baseURL}/api/feed-items?${query.join("&")}`, params);
  record(response, scene, 200, (body) => feedBodyIsValid(body, scene));
  updateCursor(scene, response);
}

function record(response, operation, expectedStatus, bodyCheck) {
  let validBody = false;
  if (response.status === expectedStatus) {
    try {
      validBody = Boolean(bodyCheck(safeJSON(response)));
    } catch (_) {
      validBody = false;
    }
  }
  const ok = response.status === expectedStatus && validBody;
  check(response, {
    [`${operation} status ${expectedStatus}`]: (result) => result.status === expectedStatus,
    [`${operation} response is valid`]: () => validBody,
  });
  businessSuccess.add(ok, { operation });
  unexpectedFailure.add(!ok, { operation });
  operationDuration.add(response.timings.duration, { operation });
}

function feedBodyIsValid(body, scene) {
  return body.scene === scene && Array.isArray(body.items) && Object.prototype.hasOwnProperty.call(body, "next_cursor");
}

function updateCursor(scene, response) {
  if (response.status !== 200) return;
  const body = safeJSON(response);
  const continuePaging = ((__VU + __ITER) % 10) < 8;
  cursors[scene] = continuePaging && body.has_more && body.next_cursor ? body.next_cursor : "";
}

function nextCursor(scene) {
  return cursors[scene] || "";
}

function userFor(data, pool) {
  if (!data || !Array.isArray(data[pool]) || data[pool].length === 0) return null;
  return data[pool][(__VU - 1) % data[pool].length];
}

function requireUser(data, operation, pool) {
  const user = userFor(data, pool);
  if (!user) throw new Error(`${operation} requires at least one account in ${pool}`);
  return user;
}

function requireTarget(values, name) {
  if (values.length === 0) throw new Error(`${name} must contain at least one positive integer`);
  return values[(__VU * 31 + __ITER * 17) % values.length];
}

function requestID(operation) {
  return `perf-${runID}-${operation}-${__VU}-${__ITER}`.slice(0, 64);
}

function jsonParams(operation, tags = {}) {
  return {
    headers: { "Content-Type": "application/json" },
    tags: { operation, ...tags },
  };
}

function authParams(token, operation, tags = {}, idempotent = false) {
  const params = jsonParams(operation, tags);
  params.headers.Authorization = `Bearer ${token}`;
  if (idempotent) params.headers["Idempotency-Key"] = requestID(operation);
  return params;
}

function safeJSON(response) {
  try {
    return response.json() || {};
  } catch (_) {
    return {};
  }
}

function monitorsRecommendation() {
  return recommendationProfiles.includes(profile);
}

function recommendationGovernanceCounters() {
  const selector = 'sum(gcfeed_governance_events_total{component="recommendation",decision=~"fallback|circuit_open|bulkhead_full|partial_recall"}) by (decision)';
  const response = http.get(
    `${prometheusURL}/api/v1/query?query=${encodeURIComponent(selector)}`,
    { tags: { operation: "prometheus_governance" } },
  );
  const body = safeJSON(response);
  if (response.status !== 200 || body.status !== "success" || !Array.isArray(body.data && body.data.result)) {
    throw new Error(`failed to query recommendation governance counters: status=${response.status} body=${response.body}`);
  }

  const counters = Object.fromEntries(recommendationGovernanceDecisions.map((decision) => [decision, 0]));
  for (const item of body.data.result) {
    const decision = item.metric && item.metric.decision;
    const value = Array.isArray(item.value) ? Number(item.value[1]) : NaN;
    if (Object.prototype.hasOwnProperty.call(counters, decision) && Number.isFinite(value)) {
      counters[decision] = value;
    }
  }
  return counters;
}

function parseDefaultAccounts() {
  if (__ENV.ACCOUNTS_JSON) {
    return parseAccountsJSON("ACCOUNTS_JSON", __ENV.ACCOUNTS_JSON);
  }
  if (__ENV.ACCOUNT && __ENV.PASSWORD) {
    return [{ account: __ENV.ACCOUNT, password: __ENV.PASSWORD }];
  }
  return [];
}

function parseAccountPool(name, fallback) {
  return __ENV[name] ? parseAccountsJSON(name, __ENV[name]) : fallback;
}

function parseAccountsJSON(name, content) {
  const parsed = JSON.parse(content);
  if (!Array.isArray(parsed)) throw new Error(`${name} must be a JSON array`);
  return parsed.filter((item) => item && item.account && item.password);
}

function assertDisjointPools(readPool, writePool) {
  const accountKey = (account) => String(account).trim().toLowerCase();
  const readers = new Set(readPool.map((item) => accountKey(item.account)));
  const overlap = writePool
    .map((item) => item.account)
    .filter((account) => readers.has(accountKey(account)));
  if (overlap.length > 0) {
    throw new Error(`read/write account pools must be disjoint for ${profile}: ${overlap.join(",")}`);
  }
}

function idList(name) {
  return (__ENV[name] || "")
    .split(",")
    .map((value) => Number(value.trim()))
    .filter((value) => Number.isInteger(value) && value > 0);
}

function env(name, fallback) {
  return __ENV[name] || fallback;
}

function positiveNumber(name, fallback) {
  const value = Number(__ENV[name] || fallback);
  if (!Number.isFinite(value) || value <= 0) throw new Error(`${name} must be a positive number`);
  return value;
}

function arrival(exec, rate, duration, preAllocatedVUs) {
  return {
    executor: "constant-arrival-rate",
    rate,
    timeUnit: "1s",
    duration,
    preAllocatedVUs,
    maxVUs: Math.max(preAllocatedVUs, rate * 4),
    exec,
  };
}

function scheduledArrival(exec, rate, duration, startTime, fallbackVUs) {
  const preAllocatedVUs = positiveNumber("PRE_ALLOCATED_VUS", fallbackVUs);
  return {
    ...arrival(exec, rate, duration, preAllocatedVUs),
    startTime,
    maxVUs: positiveNumber("MAX_VUS", Math.max(preAllocatedVUs, rate * 4)),
    gracefulStop: "0s",
  };
}

function scheduledSteps(name, rates, duration, exec) {
  const durationSeconds = parseDurationSeconds(duration);
  return Object.fromEntries(rates.map((rate, index) => [
    `${name}_${rate}`,
    scheduledArrival(exec, rate, duration, `${durationSeconds * index}s`, Math.max(100, rate * 2)),
  ]));
}

function parseDurationSeconds(duration) {
  const match = /^(\d+)(s|m|h)$/.exec(duration);
  if (!match) throw new Error(`STAGE_DURATION must use whole seconds, minutes, or hours: ${duration}`);
  const factor = { s: 1, m: 60, h: 3600 }[match[2]];
  return Number(match[1]) * factor;
}

function baselineScenario(scene) {
  return {
    scenarios: {
      [`baseline_${scene}`]: arrival(scene, positiveNumber("RPS", 10), env("DURATION", "5m"), 30),
    },
  };
}

function mixedScenarios(duration) {
  return {
    scenarios: {
      timeline: arrival("timeline", 45, duration, 100),
      hot: arrival("hot", 15, duration, 40),
      recommend: arrival("recommend", 20, duration, 80),
      exposure: arrival("exposure", 10, duration, 30),
      like: arrival("like", 5, duration, 20),
      follow: arrival("follow", 3, duration, 15),
      publish: arrival("publish", 2, duration, 10),
    },
  };
}

function thresholds(selectedProfile) {
  const result = {
    business_success_rate: ["rate>0.99"],
    unexpected_failure_rate: ["rate<0.01"],
    dropped_iterations: ["count==0"],
  };
  if (selectedProfile === "governance") return result;
  result.http_req_failed = ["rate<0.01"];
  if (recommendationProfiles.includes(selectedProfile)) {
    result.recommendation_degradation = ["count==0"];
  }
  result["operation_duration{operation:timeline}"] = ["p(95)<300", "p(99)<3000"];
  result["operation_duration{operation:hot}"] = ["p(95)<300", "p(99)<3000"];
  result["operation_duration{operation:recommend}"] = ["p(95)<500", "p(99)<3000"];
  result["operation_duration{operation:exposure}"] = ["p(99)<3000"];
  result["operation_duration{operation:like}"] = ["p(99)<3000"];
  result["operation_duration{operation:follow}"] = ["p(99)<3000"];
  result["operation_duration{operation:publish}"] = ["p(99)<3000"];
  return result;
}
