"""
Aruba Central ingestion Python package.

Current lambda: clients ingestion only (devices split or removed).
Modules:
- ingestion_handler: Client ingestion Lambda entry (append-only inserts)
- api_client: Aruba Central API client (auth, pagination, retries)
- db: MySQL repository (clients schema + append-only inserts)
"""
__all__ = ["ingestion_handler", "api_client", "db"]