# 🚚 Bulker-server HTTP API

> **See also**
> [Configure Bulker server](./server-config.md)

## Authorization

All request should have `Authorization` header with value `Bearer ${token}`. Token should be one of the tokens defined in `BULKER_AUTH_TOKENS` env variable, 
[see configuration manual](./server-config.md#bulker_auth_tokens).

## `POST /post/:destinationId?tableName=`

This the main bulker endpoint. It accepts JSON object and sends it down to procssing pipeline.

The body of the request is JSON object representing single event. The response is either `{"success": true}` or `{"success": false, "error": ""}`

`tableName` indicates which table in destination should be used. This is mandatory parameter.

### `GET /ready`

Returns `HTTP 200` if server is ready to accept requests. Otherwise, returns `HTTP 503`. Userfull
for health checks.

### `GET /metrics`

Returns metrics for prometheus export
