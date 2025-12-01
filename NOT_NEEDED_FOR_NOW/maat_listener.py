class MaatListenerOperator(MaatAPIOperator):
    """
    Specialized operator for Listener Management operations.

    :param listener_id: Listener ID (required for retrieve, delete operations)
    :param operation: Operation type ('list', 'register', 'retrieve', 'unregister')
    :param callback_url: Callback URL (required for register operation)
    :param query: Query string (optional for register operation)
    """

    def __init__(
            self,
            *,
            operation: str,
            listener_id: Optional[str] = None,
            callback_url: Optional[str] = None,
            query: Optional[str] = None,
            **kwargs
    ) -> None:
        # Determine endpoint and method based on operation
        operation = operation.lower()

        if operation == 'list':
            endpoint = '/hub'
            method = 'GET'
            data = None
            qp = {}
        elif operation == 'register':
            if not callback_url:
                raise AirflowException("callback_url is required for register operation")
            endpoint = '/hub'
            method = 'POST'
            data = {'callback': callback_url}
            if query:
                data['query'] = query
            qp = {}
        elif operation == 'retrieve':
            if not listener_id:
                raise AirflowException("listener_id is required for retrieve operation")
            endpoint = f'/hub/{listener_id}'
            method = 'GET'
            data = None
            qp = {}
        elif operation == 'unregister':
            if not listener_id:
                raise AirflowException("listener_id is required for unregister operation")
            endpoint = f'/hub/{listener_id}'
            method = 'DELETE'
            data = None
            qp = {}
        else:
            raise AirflowException(
                f"Invalid operation: {operation}. Must be one of: list, register, retrieve, unregister"
            )

        super().__init__(
            endpoint=endpoint,
            method=method,
            data=data,
            query_params=qp,
            **kwargs
        )
"""Custom Airflow operators for netdevops workflows."""