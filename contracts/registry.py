"""Minimal Schema Registry client (Confluent REST API, served by Redpanda). Stdlib only.

An unknown schema id is a property of the *message* (it goes to the DLQ). A registry that does
not answer is an *infrastructure* failure: it is raised, so the Flink job fails and restarts
from its last checkpoint instead of dead-lettering good data.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

JSON = "application/vnd.schemaregistry.v1+json"


class RegistryUnavailable(RuntimeError):
    """The registry could not be reached or answered with a server error."""


class UnknownSchemaId(LookupError):
    def __init__(self, schema_id: int) -> None:
        super().__init__(f"schema id {schema_id} is not registered")
        self.schema_id = schema_id


class SchemaNotRegistered(LookupError):
    """The producer's schema is not registered under its subject (no auto-registration)."""


class Registry:
    def __init__(self, url: str, timeout: float = 5.0) -> None:
        self.url, self.timeout = url.rstrip("/"), timeout
        self._by_id: dict[int, dict] = {}

    def _call(self, method: str, path: str, body: dict | None = None) -> tuple[int, Any]:
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            self.url + path, data=data, method=method, headers={"Content-Type": JSON}
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return resp.status, json.loads(resp.read() or b"null")
        except urllib.error.HTTPError as exc:
            if exc.code >= 500:
                raise RegistryUnavailable(f"{method} {path}: HTTP {exc.code}") from exc
            return exc.code, json.loads(exc.read() or b"null")
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise RegistryUnavailable(f"{method} {path}: {exc}") from exc

    def schema_by_id(self, schema_id: int) -> dict:
        if schema_id not in self._by_id:
            status, body = self._call("GET", f"/schemas/ids/{schema_id}")
            if status == 404:
                raise UnknownSchemaId(schema_id)
            if status != 200:
                raise RegistryUnavailable(f"GET /schemas/ids/{schema_id}: HTTP {status}")
            self._by_id[schema_id] = json.loads(body["schema"])
        return self._by_id[schema_id]

    def id_for(self, subject: str, schema: dict) -> int:
        """Id of an already registered schema. Never registers (the contract lives in git)."""
        status, body = self._call("POST", f"/subjects/{subject}", {"schema": json.dumps(schema)})
        if status == 404:
            raise SchemaNotRegistered(
                f"schema is not registered under {subject!r}; run `make up` (redpanda-init)"
            )
        if status != 200:
            raise RegistryUnavailable(f"POST /subjects/{subject}: HTTP {status} {body}")
        return int(body["id"])

    def compatibility(self, subject: str) -> str | None:
        status, body = self._call("GET", f"/config/{subject}")
        return body.get("compatibilityLevel") if status == 200 else None
