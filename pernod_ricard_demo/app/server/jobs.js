// Databricks Jobs API client for pipeline management

let _cachedToken = null;
let _tokenExpiry = 0;

function getHost() {
  let host = process.env.DATABRICKS_HOST || 'https://fevm-serverless-opm.cloud.databricks.com';
  if (!host.startsWith('http')) host = `https://${host}`;
  return host.replace(/\/$/, '');
}

async function getToken() {
  if (process.env.DATABRICKS_TOKEN) {
    return process.env.DATABRICKS_TOKEN;
  }

  if (_cachedToken && Date.now() < _tokenExpiry) {
    return _cachedToken;
  }

  const clientId = process.env.DATABRICKS_CLIENT_ID;
  const clientSecret = process.env.DATABRICKS_CLIENT_SECRET;

  if (clientId && clientSecret) {
    const host = getHost();
    const response = await fetch(`${host}/oidc/v1/token`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        grant_type: 'client_credentials',
        client_id: clientId,
        client_secret: clientSecret,
        scope: 'all-apis',
      }),
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Failed to get OAuth token: ${response.status}`);
    }

    const data = await response.json();
    _cachedToken = data.access_token;
    _tokenExpiry = Date.now() + ((data.expires_in || 3600) - 60) * 1000;
    return _cachedToken;
  }

  return '';
}

async function databricksAPI(method, path, body = null) {
  const host = getHost();
  const token = await getToken();

  const options = {
    method,
    headers: {
      'Authorization': `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
  };
  if (body) {
    options.body = JSON.stringify(body);
  }

  const response = await fetch(`${host}${path}`, options);
  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Databricks API error (${response.status}): ${errorText}`);
  }
  return response.json();
}

/**
 * Find the [dev chaima_berrachdi] [development] Pernod Ricard - Full Pipeline job by name, then trigger a run.
 */
export async function triggerPipeline() {
  const jobsList = await databricksAPI('GET', '/api/2.1/jobs/list?name=[dev chaima_berrachdi] [development] Pernod Ricard - Full Pipeline');
  const jobs = jobsList.jobs || [];

  if (jobs.length === 0) {
    throw new Error('Job "[dev chaima_berrachdi] [development] Pernod Ricard - Full Pipeline" not found. Please create it first.');
  }

  const jobId = jobs[0].job_id;

  const runResult = await databricksAPI('POST', '/api/2.1/jobs/run-now', {
    job_id: jobId,
  });

  return {
    success: true,
    run_id: runResult.run_id,
    job_id: jobId,
    message: 'Pipeline triggered successfully',
  };
}

/**
 * List recent job runs for the [dev chaima_berrachdi] [development] Pernod Ricard - Full Pipeline job.
 */
export async function listJobRuns() {
  const jobsList = await databricksAPI('GET', '/api/2.1/jobs/list?name=[dev chaima_berrachdi] [development] Pernod Ricard - Full Pipeline');
  const jobs = jobsList.jobs || [];

  if (jobs.length === 0) {
    return [];
  }

  const jobId = jobs[0].job_id;

  const runsResult = await databricksAPI(
    'GET',
    `/api/2.1/jobs/runs/list?job_id=${jobId}&limit=10&expand_tasks=false`
  );

  const runs = (runsResult.runs || []).map(run => ({
    run_id: run.run_id,
    state: run.state?.life_cycle_state || 'UNKNOWN',
    result_state: run.state?.result_state || null,
    start_time: run.start_time,
    end_time: run.end_time || null,
    duration_ms: run.run_duration || null,
    trigger: run.trigger || 'MANUAL',
  }));

  return runs;
}
