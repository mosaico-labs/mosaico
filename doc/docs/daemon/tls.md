# TLS Support

Securing your Mosaico instance is straightforward, as TLS (Transport Layer Security) is fully supported out of the box. 
Enabling TLS ensures that all communications with the daemon are encrypted and secure.

To activate it, simply append the `--tls` flag to your `mosaicod run` command.

When the `--tls` flag is used, `mosaicod` requires a valid certificate and private key. 

It looks for these credentials via the following environment variables:

* `MOSAICOD_TLS_CERT_FILE`: The path to the PEM-encoded X.509 certificate.
* `MOSAICOD_TLS_PRIVATE_KEY_FILE`: The path to the file containing the PEM-encoded RSA private key.

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

!!! warning "Test certificate"
    The certificates produced by the command above is strictly for local development or testing. Do not use it in production.

