# Authorization Policies

In Mosaico, access control is managed through **Authorization Policies**. An Authorization Policy securely binds a unique API key to a specific set of permissions.

!!! note "Scope of access"

    It is important to note that Authorization Policies in Mosaico act as a *loose, coarse-grained access mechanism*. They are designed strictly to limit the *types of operations* an API key owner can perform across the platform as a whole.

    This mechanism *does not support fine-grained policies*. You cannot use an Authorization Policy to restrict an API key's access to specific resources, such as individual topics or sequences. For example, if an API key is granted the `read` permission, the client is allowed to read data globally across the entire platform, rather than being restricted to a single topic or a specific subset of data.


## Properties

Each Authorization Policy in Mosaico acts as a single access control rule and consists of the following properties:

| Property | Status | Description |
| --- | --- | --- |
| **API Key** | Required | The unique token provided by the client to authenticate with the Mosaico platform. |
| **Permissions** | Required | A defined list of global operations (e.g., `read`, `write`) the owner of the API key is authorized to perform. |
| **Description** | Required | A human-readable text string explaining the specific purpose or use case of the policy. |
| **Creation Time** | Auto-generated | The exact timestamp when the API key and its associated policy were generated. |
| **Expiration Time** | Optional | A predetermined date and time after which the API key automatically becomes invalid. |

## API Key Structure

Mosaico API keys follow a strict three-part format separated by underscores (`_`):

```text
[header]_[payload]_[fingerprint]
```

**Example:** `msco_vrfeceju4lqivysxgaseefa3tsxs0vrl_1b676530`

**Header**. A fixed prefix that easily identifies the token as a Mosaico API key. This helps developers quickly spot Mosaico tokens in configuration files or environment variables.

**Payload**. The actual secret token. It is a 32-byte string consisting entirely of lowercase, alphanumeric ASCII characters. **This payload must be kept secret and should never be exposed in client-side code.**

**Fingerprint**. An hash of the payload. Mosaico uses the fingerprint to identify the key within the system without needing to handle or log the raw secret payload. It is primarily used for administrative actions, such as checking the key's status or revoking it.

## Available Permissions

Permissions dictate the exact global operations an API key can execute. Mosaico follows the principle of least privilege, meaning you should only assign the permissions strictly necessary for the client's function.

| Permission | Action Allowed | Typical Use Case |
| --- | --- | --- |
| `read` | Retrieve and view data from anywhere on the Mosaico platform. | Front-end dashboards, data analytics integrations, or public-facing read-only APIs. |
| `write` | Create new data or update existing data anywhere on the platform. | Ingestion scripts, user input forms, or webhooks pushing data to Mosaico. |
| `delete` | Permanently remove data from the platform. | Data lifecycle management, GDPR compliance scripts, or cleanup tasks. |
| `manage` | Perform administrative operations on the platform. | Rotating/revoking API keys, managing users, or running automated maintenance tasks. |
