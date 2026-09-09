# Security model

This alpha is for a **single trusted team**, not a public multi-tenant service. Asset code executes with the operating-system permissions, environment, network access, and PostgreSQL credentials of its worker. Subprocesses are not security sandboxes.

The CLI binds to loopback by default. Binding another interface requires `DORC_API_TOKEN` or an explicit `--allow-unauthenticated`. Docker Compose exposes only loopback ports and explicitly allows unauthenticated local development. Set a strong token, change the development database password, and use TLS plus a trusted reverse proxy before allowing remote access. The browser stores its bearer token in session storage for the current tab. All token holders have the same privileges; scoped credentials, users, RBAC, SSO, and token rotation workflows are not implemented.

The API only executes previously registered definitions; clients cannot submit Python entrypoints or upload code. Same-origin checks protect browser writes, and the UI uses a restrictive content security policy. These controls do not make untrusted asset code safe.

Logs and previews may contain business data or secrets supplied by asset code. Do not log credentials. There is no automatic field-level redaction. The event journal records engine and control actions, but does not provide named-user attribution or tamper-evident audit guarantees. PostgreSQL backups and access controls remain the operator's responsibility.

For a suspected vulnerability, use the repository's private vulnerability reporting feature when enabled. Do not include credentials, private datasets, or working exploit payloads in a public issue. No production-security support SLA is offered for the alpha.
