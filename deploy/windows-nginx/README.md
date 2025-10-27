Windows NGINX deployment for tm-app (client + NestJS server)

This folder contains a ready-to-use NGINX configuration for hosting the tm-app client (static files) with the Node/NestJS API behind a reverse proxy on Windows.

What you get
- Production NGINX config (HTTP), optional HTTPS config.
- Security and gzip snippets.
- Proxy settings tuned for Node.
- SPA fallback for the client, long-lived cache for assets.

Assumptions
- Server (NestJS) listens on 127.0.0.1:4000 and exposes API under /api (default in tm-app/server/src/main.ts).
- Client build output is tm-app/client/dist (Vite default). Adjust root if yours differs.

1) Build and run the server (NestJS)
- Edit tm-app/server/.env and ensure:
  - PORT=4000
  - API_PREFIX=api
- From tm-app/server:
  - npm ci
  - npm run build
  - Run once to verify: node dist/main.js
  - You should see: Server listening on http://localhost:4000/api

Option A: Run server as a Windows Service with NSSM (recommended)
- Download NSSM from https://nssm.cc/download and unzip (e.g., C:\tools\nssm)
- Open an elevated PowerShell:
  - C:\tools\nssm\win64\nssm.exe install tm-app-server
    - Application: C:\Program Files\nodejs\node.exe
    - Arguments:   C:\path\to\repo\tm-app\server\dist\main.js
    - AppDirectory: C:\path\to\repo\tm-app\server
  - Start it: C:\tools\nssm\win64\nssm.exe start tm-app-server

Option B: Run with a console (for testing only)
- node tm-app/server/dist/main.js

2) Build the client
- The client expects NEXT_PUBLIC_API_URL at build time. Set it to your public origin (same as NGINX), e.g. https://example.com
- From tm-app/client (PowerShell):
  - $env:NODE_ENV = 'production'
  - $env:VITE_API_URL = 'http://localhost:7000'
  - npm ci
  - npm run build
 - Verify build at tm-app/client/dist

3) Install NGINX on Windows
- Download Windows NGINX (stable) from https://nginx.org/en/download.html
- Unzip to C:\nginx
- Stop any existing NGINX instance: C:\nginx\nginx.exe -s stop (ignore if first time)

Optional: run NGINX as a Windows Service (via NSSM)
- C:\tools\nssm\win64\nssm.exe install nginx
  - Application: C:\nginx\nginx.exe
  - AppDirectory: C:\nginx
  - Arguments: (leave empty)
- Start: C:\tools\nssm\win64\nssm.exe start nginx

4) Copy these configs
- Copy tm-app/deploy/windows-nginx/nginx.conf            -> C:\nginx\conf\nginx.conf
- Copy tm-app/deploy/windows-nginx/conf.d\*.conf        -> C:\nginx\conf\conf.d\
- Copy tm-app/deploy/windows-nginx/snippets\*.conf      -> C:\nginx\conf\snippets\
- Ensure folders exist (create conf.d and snippets if missing).

5) Point NGINX to your client build
- Edit C:\nginx\conf\conf.d\tm-app-http.conf
  - Set the root directive to your build path, for example:
    root  C:/path/to/repo/tm-app/client/dist;

6) Optional: Enable HTTPS
- Edit C:\nginx\conf\conf.d\tm-app-https.conf:
  - Set server_name to your domain.
  - Set ssl_certificate and ssl_certificate_key to your certificate files.
- Obtain free TLS certs on Windows with win-acme (Let’s Encrypt) https://www.win-acme.com/
  - After issuance, point ssl_certificate to fullchain.pem and ssl_certificate_key to privkey.pem.
- Start using the HTTPS server block and consider enabling the HTTP->HTTPS redirect block.

7) Open Windows Firewall ports
- Allow inbound TCP 80 (and 443 if using HTTPS).

8) Start and validate NGINX
- Start:  C:\nginx\nginx.exe
- Test:   C:\nginx\nginx.exe -t
- Reload: C:\nginx\nginx.exe -s reload
- Health: http://localhost/nginx-health should return "ok".

Layout summary
- Client (static): served by NGINX from the root you configure.
- API: proxied to http://127.0.0.1:4000, path /api preserved.
- Server static (/static/permis_templates): proxied to Node as implemented in server/src/main.ts.

Maintenance tips
- Log files: C:\nginx\logs\tm-app.access.log, C:\nginx\logs\tm-app.error.log
- To rotate logs: rename logs then run C:\nginx\nginx.exe -s reopen (use Task Scheduler).
- If you change server/.env (PORT/API_PREFIX), update this NGINX config to match.
