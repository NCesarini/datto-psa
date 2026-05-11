const path = require("path");
const { apps } = require(path.join(__dirname, "..", "mcp-servers.ecosystem.config.js"));

module.exports = {
  apps: apps.filter((app) => app.name === "mcp-datto-psa"),
};
