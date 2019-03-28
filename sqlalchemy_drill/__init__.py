__version__ = '0.9'
from sqlalchemy.dialects import registry

registry.register("drill", "sqlalchemy_drill.sadrill", "DrillDialect_sadrill")
registry.register("drill.sadrill", "sqlalchemy_drill.sadrill", "DrillDialect_sadrill")

registry.register("drill.jdbc", "sqlalchemy_drill.jdbc", "DrillDialect_jdbc")




