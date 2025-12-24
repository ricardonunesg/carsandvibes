/* watch-job.mjs — Node v20+ */

const endpoint = process.env.ADMIN_API_URL ?? 'http://127.0.0.1:3000/admin-api';
const username = process.env.VENDURE_USER ?? 'superadmin';
const password = process.env.VENDURE_PASS ?? 'superadmin';

const args = Object.fromEntries(
  process.argv.slice(2).reduce((acc, cur, i, arr) => {
    if (cur.startsWith('--')) {
      const [k, v] = cur.split('=');
      if (v !== undefined) acc.push([k.slice(2), v]);
      else if (arr[i + 1] && !arr[i + 1].startsWith('--')) acc.push([k.slice(2), arr[i + 1]]);
      else acc.push([k.slice(2), true]);
    }
    return acc;
  }, [])
);

const jobId = args.jobId || args.id || process.env.JOB_ID;
const intervalMs = Number(args.intervalMs ?? 1000);

if (!jobId) {
  console.error('Falta o --jobId (ou env JOB_ID). Ex: node watch-job.mjs --jobId 145');
  process.exit(1);
}

// ---------- GraphQL ----------
const LOGIN_MUT = `
  mutation ($u:String!, $p:String!, $remember:Boolean!) {
    login(username:$u, password:$p, rememberMe:$remember) {
      __typename
      ... on CurrentUser { id identifier }
      ... on InvalidCredentialsError { errorCode message }
      ... on NativeAuthStrategyError { errorCode message }
    }
  }
`;

const JOB_QUERY = `
  query ($id: ID!) {
    job(jobId: $id) {
      id
      state
      progress
      queueName
      isSettled
      error
      result
    }
  }
`;

// ---------- simples gestor de cookies ----------
let cookieHeader = '';
function captureCookies(res) {
  const set = res.headers.get('set-cookie');
  if (set) {
    // junta cookies de respostas sucessivas (básico, suficiente p/ sessão)
    const parts = set.split(/,(?=[^ ;]+=[^;]+)/g).map(s => s.split(';')[0].trim());
    const current = cookieHeader ? cookieHeader.split('; ').filter(Boolean) : [];
    const map = new Map(current.map(p => p.split('=')));
    for (const p of parts) {
      const [n, v] = p.split('=');
      map.set(n, v);
    }
    cookieHeader = Array.from(map.entries()).map(([n, v]) => `${n}=${v}`).join('; ');
  }
}

async function gql(query, variables = {}) {
  const res = await fetch(endpoint, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...(cookieHeader ? { Cookie: cookieHeader } : {}),
    },
    body: JSON.stringify({ query, variables }),
  });
  captureCookies(res);
  const json = await res.json();
  return json;
}

async function login() {
  process.stdout.write(`A autenticar em ${endpoint}... `);
  const json = await gql(LOGIN_MUT, { u: username, p: password, remember: true });
  if (json.errors) {
    console.error('\nErro de login:', JSON.stringify(json.errors, null, 2));
    process.exit(1);
  }
  const t = json.data?.login?.__typename;
  if (t !== 'CurrentUser') {
    console.error(`\nFalha de login:`, json.data?.login);
    process.exit(1);
  }
  console.log(`OK como ${json.data.login.identifier}`);
}

function printLine(job) {
  const { state, progress, queueName, error } = job;
  const pct = typeof progress === 'number' ? `${progress}%` : '-';
  const q = queueName ? ` [${queueName}]` : '';
  if (error) {
    console.log(`${state}${q} ${pct} — ERROR: ${error}`);
  } else {
    console.log(`${state}${q} ${pct}`);
  }
}

async function pollJob(id) {
  while (true) {
    const json = await gql(JOB_QUERY, { id });
    if (json.errors) {
      console.error('Erro na query do job:', JSON.stringify(json.errors, null, 2));
      process.exit(2);
    }
    const job = json.data?.job;
    if (!job) {
      console.error('Job não encontrado ou sem permissões.');
      process.exit(3);
    }

    printLine(job);

    if (job.state === 'COMPLETED' || job.state === 'FAILED' || job.isSettled) {
      // mostra result se existir
      if (job.result) {
        console.log('Resultado:', JSON.stringify(job.result, null, 2));
      }
      break;
    }
    await new Promise(r => setTimeout(r, intervalMs));
  }
}

(async () => {
  try {
    await login();
    await pollJob(jobId);
  } catch (err) {
    console.error('Erro:', err);
    process.exit(99);
  }
})();
