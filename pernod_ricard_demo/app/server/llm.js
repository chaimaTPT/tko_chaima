// Knowledge Assistant endpoint client — uses the Agent Bricks KA for RAG-grounded answers

const KA_ENDPOINT = process.env.KA_ENDPOINT || 'ka-94303de4-endpoint';

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
      console.error(`OAuth token error (${response.status}): ${text.substring(0, 300)}`);
      throw new Error(`Failed to get OAuth token: ${response.status}`);
    }

    const data = await response.json();
    _cachedToken = data.access_token;
    _tokenExpiry = Date.now() + ((data.expires_in || 3600) - 60) * 1000;
    return _cachedToken;
  }

  console.warn('WARNING: No auth credentials available for KA endpoint.');
  return '';
}

/**
 * Chat with the Knowledge Assistant endpoint.
 */
export async function chatWithKA(userMessages) {
  const host = getHost();
  const token = await getToken();

  const input = userMessages.map(m => ({
    role: m.role,
    content: m.content,
  }));

  const url = `${host}/serving-endpoints/${KA_ENDPOINT}/invocations`;

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ input }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`KA API error (${response.status}): ${errorText}`);
  }

  const data = await response.json();

  const outputItems = data.output || [];
  let content = '';
  const sources = [];

  for (const item of outputItems) {
    if (item.type === 'message' && item.content) {
      for (const block of item.content) {
        if (block.type === 'output_text' && block.text) {
          content += block.text;
        }
        if (block.annotations) {
          for (const ann of block.annotations) {
            if (ann.type === 'url_citation' && ann.title) {
              sources.push(ann.title);
            }
          }
        }
      }
    }
  }

  if (!content) {
    content = 'No response generated. Please try rephrasing your question.';
  }

  const uniqueSources = [...new Set(sources)];

  return {
    role: 'assistant',
    content: content.trim(),
    sources: uniqueSources,
  };
}
