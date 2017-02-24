__version__ = '0.8'
from sqlalchemy.dialects import registry

registry.register("drill", "sqlalchemy_drill.pydrill", "DrillDialect_pydrill")
registry.register("drill.pydrill", "sqlalchemy_drill.pydrill", "DrillDialect_pydrill")






