import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '5s', target: 20 },
    { duration: '15s', target: 50 },
    { duration: '5s', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<200', 'p(99)<500'],
    http_req_failed: ['rate<0.01'],
  },
};

export default function () {
  const url = 'http://localhost:8080/tasks';
  const payload = JSON.stringify({
    payload: {
      user_id: `user_${__VU}_${__ITER}`,
      action: 'process_event',
      data: 'k6 load test sample data'
    },
    priority: 1,
    timeout_seconds: 10,
    max_retries: 3
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const res = http.post(url, payload, params);

  check(res, {
    'status is 201 Created': (r) => r.status === 201,
  });

  sleep(0.01);
}
