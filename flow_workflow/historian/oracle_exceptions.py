from sqlalchemy import exc


EXIT_ON = (
    exc.ResourceClosedError,
    exc.TimeoutError,
    exc.DisconnectionError,
    exc.DatabaseError,
    exc.InternalError,
)
