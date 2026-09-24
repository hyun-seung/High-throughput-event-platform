import http from 'k6/http';
import { check } from 'k6';
import execution from 'k6/execution';

const rate = Number(__ENV.POC_RATE);
const seconds = Number(__ENV.POC_SECONDS);
const body = JSON.stringify({ deliveryType: 'EMAIL', payload: { message: 'x'.repeat(960) } });
export const options = {
  scenarios: { traffic: {
    executor: 'constant-arrival-rate', rate, timeUnit: '1s', duration: `${seconds}s`,
    preAllocatedVUs: Math.max(20, rate), maxVUs: Math.min(1000, Math.max(100, rate * 5)),
    gracefulStop: '25s',
  } },
  systemTags: ['status', 'method', 'name', 'scenario', 'expected_response'],
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(50)', 'p(95)', 'p(99)'],
  thresholds: {
    dropped_iterations: [{ threshold: 'count==0', abortOnFail: true, delayAbortEval: '5s' }],
    checks: [{ threshold: 'rate==1', abortOnFail: true, delayAbortEval: '5s' }],
  },
};

export default function () {
  const iteration = execution.scenario.iterationInTest;
  const key = `${__ENV.POC_RUN_ID}-${__ENV.POC_MODE === 'duplicate' ? Math.floor(iteration / 2) : iteration}`;
  const started = Date.now();
  console.log(JSON.stringify({ kind: 'start', iteration, key, started }));
  const response = http.post(`${__ENV.POC_API}/api/v1/deliveries`, body, {
    headers: { Authorization: `Bearer ${__ENV.POC_TOKEN}`, 'Idempotency-Key': key, 'Content-Type': 'application/json' },
    timeout: '21s', tags: { name: 'delivery-submit' },
  });
  let deliveryId = null;
  try { deliveryId = response.json('data.deliveryId') || null; } catch (_) { /* retain unconfirmed inputs */ }
  console.log(JSON.stringify({ kind: 'result', iteration, key, status: response.status,
    deliveryId, durationMs: response.timings.duration, finished: Date.now(), errorCode: response.error_code || 0 }));
  check(response, { 'accepted with ID': r => r.status === 202 && deliveryId !== null });
}

export function handleSummary(data) {
  return { [__ENV.POC_SUMMARY]: JSON.stringify(data, null, 2) };
}
