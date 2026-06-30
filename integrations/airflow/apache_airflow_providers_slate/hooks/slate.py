from airflow.hooks.base import BaseHook
from slate_sdk import SlateClient


class SlateHook(BaseHook):
    """
    Airflow hook for Slate. Manages the connection to a slate-api instance.

    Set up a connection in the Airflow UI:
        Connection ID:   slate_default
        Connection Type: HTTP
        Host:            http://your-slate-api-host
        Port:            3030
    """

    conn_name_attr = "slate_conn_id"
    default_conn_name = "slate_default"
    conn_type = "slate"
    hook_name = "Slate"

    def __init__(self, slate_conn_id: str = default_conn_name):
        super().__init__()
        self.slate_conn_id = slate_conn_id
        self._client: SlateClient | None = None

    def get_conn(self) -> SlateClient:
        if self._client:
            return self._client
        conn = self.get_connection(self.slate_conn_id)
        host = conn.host or "http://localhost"
        port = conn.port or 3030
        base_url = f"{host.rstrip('/')}:{port}"
        self._client = SlateClient(base_url)
        return self._client
