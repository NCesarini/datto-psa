// PM2 ecosystem config for mcp-datto-psa (multi-tenant entry point).
//
// Quick start:
//   pm2 start ecosystem.config.js
//   pm2 save && pm2 startup        # persist across reboots
//   pm2 install pm2-logrotate      # rotate logs by size/age
//
// Day-2:
//   pm2 logs mcp-datto-psa --lines 100
//   pm2 reload mcp-datto-psa       # zero-downtime restart on code change
//   pm2 restart mcp-datto-psa --update-env  # pick up env changes
//
// IMPORTANT: this server is multi-tenant. AUTOTASK_USERNAME/SECRET/INTEGRATION_CODE
// must NOT be set on this process — credentials arrive per request from Darcy.

module.exports = {
  apps: [
    {
      name: "mcp-datto-psa",

      // Run from this directory; override if deploying elsewhere.
      cwd: __dirname,

      // Use the venv's Python directly so PM2 doesn't need to source activate.
      // Run `python3 -m venv .venv && .venv/bin/pip install -r requirements.txt`
      // once before `pm2 start`.
      script: "./.venv/bin/python",
      args: "server_multitenant.py",
      interpreter: "none",

      // Single process. The pool, semaphore, and rate limiter are all in-process;
      // cluster mode would duplicate state per worker and risk exceeding
      // Autotask's 3-thread-per-integration cap.
      instances: 1,
      exec_mode: "fork",

      // Crash handling.
      autorestart: true,
      min_uptime: "30s",       // treat anything dying within 30s as a crash loop
      max_restarts: 10,
      restart_delay: 2000,
      kill_timeout: 10000,     // give uvicorn 10s to drain on SIGTERM

      // Environment. No Autotask credentials here by design.
      env: {
        HOST: "127.0.0.1",     // bind loopback only; reverse-proxy or expose via private network
        PORT: "8765",
        LOG_LEVEL: "info",
        ACCESS_LOG: "true",

        // Multi-tenant safety: reject any tools/call without a tenant block.
        MULTI_TENANT_ONLY: "true",

        // Pool sizing. Tune to ~1.5x distinct tenants in steady state.
        DATTO_POOL_MAX: "50",
        DATTO_POOL_TTL_SEC: "1800",
      },

      // Optional production-only overrides — activate with:
      //   pm2 start ecosystem.config.js --env production
      env_production: {
        LOG_LEVEL: "info",
        ACCESS_LOG: "false",   // quieter; rely on rpc_in/rpc_out lines
      },

      // Logs (relative to cwd; ensure ./logs exists or set absolute paths).
      out_file: "./logs/mcp-datto-psa.out.log",
      error_file: "./logs/mcp-datto-psa.err.log",
      merge_logs: true,
      time: true,
    },
  ],
};
