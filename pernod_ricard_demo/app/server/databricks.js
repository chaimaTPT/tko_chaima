// Databricks SQL Statement Execution API client
// Uses Databricks App service principal OAuth for authentication

const WAREHOUSE_ID = process.env.DATABRICKS_WAREHOUSE_ID || '6457618d1c009dd6';

let _cachedToken = null;
let _tokenExpiry = 0;

function getHost() {
  let host = process.env.DATABRICKS_HOST || 'https://fevm-serverless-opm.cloud.databricks.com';
  if (!host.startsWith('http')) host = `https://${host}`;
  return host.replace(/\/$/, '');
}

/**
 * Get an OAuth token for the app's service principal.
 * In Databricks Apps, the SP credentials are available via env vars.
 */
async function getToken() {
  // If DATABRICKS_TOKEN is set directly, use it
  if (process.env.DATABRICKS_TOKEN) {
    return process.env.DATABRICKS_TOKEN;
  }

  // Check cache
  if (_cachedToken && Date.now() < _tokenExpiry) {
    return _cachedToken;
  }

  // Use OAuth M2M token exchange with SP credentials
  const clientId = process.env.DATABRICKS_CLIENT_ID;
  const clientSecret = process.env.DATABRICKS_CLIENT_SECRET;

  if (clientId && clientSecret) {
    const host = getHost();
    const tokenUrl = `${host}/oidc/v1/token`;

    const response = await fetch(tokenUrl, {
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
      console.error(`OAuth token error (${response.status}): ${text.substring(0, 300)}`);
      throw new Error(`Failed to get OAuth token: ${response.status}`);
    }

    const data = await response.json();
    _cachedToken = data.access_token;
    // Cache for slightly less than the expiry time
    _tokenExpiry = Date.now() + ((data.expires_in || 3600) - 60) * 1000;
    return _cachedToken;
  }

  console.warn('WARNING: No DATABRICKS_TOKEN, DATABRICKS_CLIENT_ID, or DATABRICKS_CLIENT_SECRET found.');
  return '';
}

/**
 * Execute a SQL statement via the Databricks SQL Statement API.
 * Returns an array of row objects.
 */
export async function executeSQL(sql) {
  const host = getHost();
  const token = await getToken();

  const response = await fetch(`${host}/api/2.0/sql/statements`, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      warehouse_id: WAREHOUSE_ID,
      statement: sql,
      wait_timeout: '30s',
      disposition: 'INLINE',
      format: 'JSON_ARRAY',
    }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    console.error(`SQL API error (${response.status}): ${errorText.substring(0, 500)}`);
    throw new Error(`Databricks SQL error (${response.status}): ${errorText}`);
  }

  const result = await response.json();

  if (result.status?.state === 'FAILED') {
    throw new Error(`SQL execution failed: ${result.status.error?.message || 'Unknown error'}`);
  }

  if (result.status?.state === 'PENDING' || result.status?.state === 'RUNNING') {
    return await pollStatement(result.statement_id);
  }

  return parseResult(result);
}

async function pollStatement(statementId) {
  const host = getHost();
  const token = await getToken();
  const maxAttempts = 30;

  for (let i = 0; i < maxAttempts; i++) {
    await new Promise(resolve => setTimeout(resolve, 1000));

    const response = await fetch(`${host}/api/2.0/sql/statements/${statementId}`, {
      headers: { 'Authorization': `Bearer ${token}` },
    });

    const result = await response.json();

    if (result.status?.state === 'SUCCEEDED') {
      return parseResult(result);
    }
    if (result.status?.state === 'FAILED') {
      throw new Error(`SQL failed: ${result.status.error?.message || 'Unknown'}`);
    }
  }
  throw new Error('SQL statement timed out');
}

function parseResult(result) {
  const columns = result.manifest?.schema?.columns || [];
  const dataArray = result.result?.data_array || [];

  return dataArray.map(row => {
    const obj = {};
    columns.forEach((col, i) => {
      obj[col.name] = row[i];
    });
    return obj;
  });
}

export async function executeSQLMulti(queries) {
  return Promise.all(queries.map(q => executeSQL(q)));
}
