---
title: TLS
sidebar_position: 9
description: "How to enable and configure TLS on mosaicod. Covers certificate and private key file setup, environment variable configuration, self-signed certificate generation, and client verification modes."
---

Securing your Mosaico instance is straightforward, as [TLS (Transport Layer Security)](https://en.wikipedia.org/wiki/Transport_Layer_Security) is fully supported out of the box. 
Enabling TLS ensures that all communications with the daemon are encrypted and secure.

To activate it, simply append the `--tls` flag to your `mosaicod run` command.

When the `--tls` flag is used, `mosaicod` requires a valid certificate and private key. 

It looks for these credentials via the following [environment variables](env.md#tls):

* `MOSAICOD_TLS_CERT_FILE`: The path to the PEM-encoded X.509 certificate.
* `MOSAICOD_TLS_PRIVATE_KEY_FILE`: The path to the file containing the PEM-encoded RSA private key.

:::tip
    If you prefer to manage TLS termination separately, you can run `mosaicod` without the `--tls` flag and use a reverse proxy (like [Nginx](https://nginx.org/) or [Caddy](https://caddyserver.com/)) to handle SSL termination. 
    This allows you to centralize TLS management and offload encryption tasks from the daemon.
:::

## Generate a Self-Signed Certificate

Run the following command to generate a `cert.pem` a `key.pem` and a `ca.pem` file:

```bash
# Generate the root CA
openssl genrsa -out ca.key 4096
openssl req -x509 -new -nodes -key ca.key -sha256 -days 365 \
  -subj "/CN=MyTestCA" -out ca.pem

# Generate the server private key
openssl genrsa -out key.pem 4096

# Create a certificate signing request (CSR) for the server
openssl req -new -key key.pem -out server.csr \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

# Sign the server CSR with the root CA to create the end-entity certificate
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
  -out cert.pem -days 365 -sha256 \
  -extfile <(printf "basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\nsubjectAltName=DNS:localhost,IP:127.0.0.1")
```

The scripts will generate the following files

|File|Description|
|---|---|
|`ca.key`|The private key for the Certificate Authority; used only to sign other certificates and **must be kept secret**.|
|`ca.pem`|The public certificate for the Authority; provided to clients so they can verify the server's identity.|
|`ca.srl`|A text file containing the next serial number to be assigned by the CA; safe to ignore or delete.|
|`key.pem`|The private key for the mosaicod server; used to decrypt traffic and prove ownership of the server certificate.|
|`cert.pem`|The end-entity certificate for the server.|
|`server.csr`|A temporary Certificate Signing Request file used to bridge the public key and the identity during generation.|

Use `cert.pem` and `key.pem` for server-side TLS identity in the mosaicod daemon, while distributing `ca.pem` to clients as the trusted root certificate.

:::warning
    The certificates produced by the command above is strictly for local development or testing. Do not use it in production.
:::

## Use a Let's Encrypt Certificate

For production deployments, you should use a publicly trusted certificate issued by [Let's Encrypt](https://letsencrypt.org/) via [Certbot](https://certbot.eff.org/). 
These certificates are free, automatically trusted by all major clients, and valid for 90 days with built-in renewal support.

### Prerequisites

* A registered domain name (e.g. `mosaico.example.com`) pointing to your server's public IP address.
* Port `80` open and reachable from the internet (required for the ACME HTTP-01 challenge).
* Certbot installed on your server.

Install Certbot on Debian/Ubuntu:

```bash
sudo apt update
sudo apt install -y certbot
```

On other platforms, refer to the [official Certbot installation guide](https://certbot.eff.org/instructions).

### Obtain a Certificate

Certbot will spin up a temporary web server to complete the domain challenge:

```bash
sudo certbot certonly --standalone -d mosaico.example.com
```

If you already have a web server (Nginx, Apache, etc.) running on port 80, use the webroot or appropriate plugin instead. 
See the [Certbot documentation](https://eff-certbot.readthedocs.io/) for details.

Upon success, Certbot writes the certificate files to `/etc/letsencrypt/live/mosaico.example.com/`:

| File | Description |
|---|---|
| `fullchain.pem` | The server certificate concatenated with the Let's Encrypt intermediate chain. Use this as your certificate file. |
| `privkey.pem` | The private key for the certificate. Keep this secret. |
| `cert.pem` | The server certificate alone (without the chain). |
| `chain.pem` | The intermediate certificate chain only. |

### Configure mosaicod

Point `mosaicod` to the Certbot-managed files using the TLS environment variables:

```bash
export MOSAICOD_TLS_CERT_FILE=/etc/letsencrypt/live/mosaico.example.com/fullchain.pem
export MOSAICOD_TLS_PRIVATE_KEY_FILE=/etc/letsencrypt/live/mosaico.example.com/privkey.pem

mosaicod run --tls
```

:::tip
    Use `fullchain.pem` rather than `cert.pem` for the certificate file. It includes the full certificate chain, which is required for clients to validate the certificate correctly without needing to fetch intermediate certificates themselves.
:::

### Renew Automatically

Let's Encrypt certificates expire after 90 days. Certbot installs a `systemd` timer (or cron job) that attempts renewal automatically. You can verify it is active with:

```bash
sudo systemctl status certbot.timer
```

Because `mosaicod` reads the certificate files from disk on startup, you need to restart it after each renewal so it picks up the new certificate. 

Add a deploy hook to do this automatically:
```bash
sudo mkdir -p /etc/letsencrypt/renewal-hooks/deploy
sudo tee /etc/letsencrypt/renewal-hooks/deploy/restart-mosaicod.sh > /dev/null <<'EOF'
#!/bin/bash
systemctl restart mosaicod
EOF
sudo chmod +x /etc/letsencrypt/renewal-hooks/deploy/restart-mosaicod.sh
```

Certbot will execute this script every time a certificate is successfully renewed.

To test the renewal process without actually replacing the certificate, run:

```bash
sudo certbot renew --dry-run
```

:::warning
    The `/etc/letsencrypt/live/` directory is owned by `root`. 
    If you run `mosaicod` as a non-root user, make sure that user has read access to the certificate files, or copy them to a location your service user can reach and update your deploy hook accordingly.
:::