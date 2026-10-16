import { strict as assert } from 'node:assert';
import { test } from 'node:test';
import { fillLaunchConfig } from '../launchConfig';

const entry = { type: 'redpanda-connect', request: 'launch', name: 'Debug pipeline' };

test('fills a missing pipeline from the active editor', () => {
  assert.deepEqual(fillLaunchConfig(entry, 'redpanda-connect', '/w/p.yaml'), {
    ...entry,
    pipeline: '/w/p.yaml',
  });
});

test('keeps a pipeline that is set', () => {
  const config = { ...entry, pipeline: 'other.yaml' };
  assert.deepEqual(fillLaunchConfig(config, 'redpanda-connect', '/w/p.yaml'), config);
});

test('F5 with no launch.json gets a launch entry for the active file', () => {
  assert.deepEqual(fillLaunchConfig({}, 'redpanda-connect', '/w/p.yaml'), {
    ...entry,
    pipeline: '/w/p.yaml',
  });
});

test('adds nothing without an active file', () => {
  assert.deepEqual(fillLaunchConfig(entry, 'redpanda-connect', undefined), entry);
});
