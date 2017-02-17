from sqlalchemy.dialects import registry

registry.register("drill", "sqlalchemy_drill.pyodbc", "DrillDialect_pyodbc")
registry.register("drill.pyodbc", "sqlalchemy_drill.pyodbc", "DrillDialect_pyodbc")
registry.register("drill.pydrill", "sqlalchemy_drill.pydrill", "DrillDialect_pydrill")

from sqlalchemy.testing import runner

runner.main()
