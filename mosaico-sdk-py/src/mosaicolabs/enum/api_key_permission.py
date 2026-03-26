from enum import Enum


class APIKeyPermissionEnum(Enum):
    Read = "read"
    """
    Read-Only access to resources

    This permission allows to:
    - List resources
    - Retrieve Sequences, Topics and the related Data streams
    - Query the data catalogs via the [`MosaicoClient.query()`][mosaicolabs.comm.MosaicoClient.query] method
    """

    Write = "write"
    """
    Write access to resources

    This permission allows to:
    - List resources
    - Retrieve Sequences, Topics and the related Data streams
    - Query the data catalogs via the [`MosaicoClient.query()`][mosaicolabs.comm.MosaicoClient.query] method
    - Create and update Sequences
    """

    Delete = "delete"
    """
    Delete access to resources

    This permission allows to:
    - List resources
    - Retrieve Sequences, Topics and the related Data streams
    - Query the data catalogs via the [`MosaicoClient.query()`][mosaicolabs.comm.MosaicoClient.query] method
    - Create and update Sequences
    - Delete Sequences, Sessions and Topics
    """

    Manage = "manage"
    """
    Full access to resources

    This permission allows to:
    - List resources
    - Retrieve Sequences, Topics and the related Data streams
    - Query the data catalogs via the [`MosaicoClient.query()`][mosaicolabs.comm.MosaicoClient.query] method
    - Create and update Sequences
    - Delete Sequences, Sessions and Topics
    - Manage API keys (create, retrieve the status, revoke)
    """
