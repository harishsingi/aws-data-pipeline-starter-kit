"""Lambda ingest package.

Modules are imported as top-level modules (``from config import ...``)
because the Lambda handler is ``app.handler`` and the deployment zip root
(``lambda/src``) is on ``sys.path`` -- relative imports do not work for a
top-level handler module. The unit tests load the modules the same way.
"""

