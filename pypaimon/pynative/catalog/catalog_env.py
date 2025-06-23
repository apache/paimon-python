from dataclasses import dataclass


class LockFactory:
    pass


class MetastoreClientFactory:
    pass


@dataclass
class CatalogEnvironment:
    lock_factory: LockFactory
    metastore_client_factory: MetastoreClientFactory

