// PM2 ecosystem — self-contained (works when this folder is deployed alone, e.g. /home/ec2-user/datto-psa).
// Monorepo: from repo root use `pm2 start mcp-servers.ecosystem.config.js` to start all MCP apps.
//
// Quick start (this directory only):
//   pm2 start ecosystem.config.js
//
// IMPORTANT: do not set AUTOTASK_USERNAME / SECRET / INTEGRATION_CODE here — credentials are per-request from Darcy.

/** Same process defaults as other MCPs on the host (e.g. *:3999). */
const sharedProcess = {
  instances: 1,
  exec_mode: "fork",
  autorestart: true,
  min_uptime: "30s",
  max_restarts: 10,
  restart_delay: 2000,
  kill_timeout: 10000,
  merge_logs: true,
  time: true,
};

module.exports = {
  apps: [
    {
      ...sharedProcess,
      name: "mcp-datto-psa",
      cwd: __dirname,
      script: "./.venv/bin/python",
      args: "server_multitenant.py",
      interpreter: "none",
      env: {
        HOST: "0.0.0.0",
        PORT: "8765",
        LOG_LEVEL: "info",
        ACCESS_LOG: "true",
        PYTHONUNBUFFERED: "1",
        MULTI_TENANT_ONLY: "true",
        DATTO_POOL_MAX: "50",
        DATTO_POOL_TTL_SEC: "1800",
      },
      env_production: {
        LOG_LEVEL: "info",
        ACCESS_LOG: "false",
      },
      out_file: "./logs/mcp-datto-psa.out.log",
      error_file: "./logs/mcp-datto-psa.err.log",
    },
  ],
};
