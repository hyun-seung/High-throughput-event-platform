import http from 'k6/http';
import { check } from 'k6';
import execution from 'k6/execution';

const rate = Number(__ENV.POC_RATE);
const seconds = Number(__ENV.POC_SECONDS);
// Load generation is k6; external Python runners only arrange services and reconcile results.
const preAllocatedVUs = Number(__ENV.POC_PREALLOCATED_VUS || Math.min(1000, Math.max(20, rate)));
const maxVUs = Number(__ENV.POC_MAX_VUS || Math.max(preAllocatedVUs, Math.min(1000, Math.max(100, rate * 5))));
if (![rate, seconds, preAllocatedVUs, maxVUs].every(n => Number.isSafeInteger(n) && n > 0)
    || maxVUs < preAllocatedVUs) {
  throw new Error('Positive integer rate/duration/VUs and maxVUs >= preAllocatedVUs are required');
}
const body = JSON.stringify({ deliveryType: 'EMAIL', payload: { message: 'x'.repeat(960) } });
export const options = {
  scenarios: { traffic: {
    executor: 'constant-arrival-rate', rate, timeUnit: '1s', duration: `${seconds}s`,
    preAllocatedVUs, maxVUs,
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
