# drill/base.py

"""
Support for the Apache Drill database.

"""
from sqlalchemy import processors
from sqlalchemy import sql, types, exc, pool, VARCHAR
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler


class DrillExecutionContext(default.DefaultExecutionContext):
    pass


class DrillCompiler(compiler.SQLCompiler):
    ## Added On Purpose
    def default_from(self):
        """Called when a ``SELECT`` statement has no froms,
        and no ``FROM`` clause is to be appended.
       Drill uses FROM values(1)
        """
        return " FROM (values(1))"

    # Strip schema
    def visit_table(self, table, asfrom=False, **kwargs):
        if asfrom:
            return self.preparer.quote(table.name, '`')
        else:
            return ""
    
    def visit_join(self, join, asfrom=False, **kwargs):
        mydebug = 0
        # The main goal of this visit_join is to pull apart the JOIN and add the table aliases to the ON Clause as Drill is finding ambiguous columns. 
        # Pulls apart the JOIN. Todo: More work to understand what we know about a JOIN and ensure we catching all cases        
        left_raw = join.left._compiler_dispatch(self, asfrom=True, **kwargs)
        right_raw = join.right._compiler_dispatch(self, asfrom=True, **kwargs)
        onclause_raw = join.onclause._compiler_dispatch(self, **kwargs)

        if onclause_raw.lower().find(' and ') >= 0:
            print "----#####  onclause has an AND"
            print "----#####  onclause: %s" % onclause_raw

        if mydebug == 1:
            print "==========================================="
            print "Left Raw: %s" % left_raw
            print "Right Raw: %s" % right_raw
            print "Onclause  Raw: %s" % onclause_raw
            print "==========================================="
            print "alias: %s" % join.alias
            print "bind: %s" % join.bind
            print "c: %s" % join.c
            print "columns: %s" % join.columns
            print "compare: %s" % join.compare
            print "compile: %s" % join.compile
            print "correspond_on_equivalents: %s" % join.correspond_on_equivalents
            print "corresponding_column: %s" % join.corresponding_column
            print "count: %s" % join.count
            print "description: %s" % join.description
            print "foreign_keys: %s" % join.foreign_keys
            print "get_childreb: %s" % join.get_children
            print "is_selectable: %s" % join.is_selectable
            print "isouter: %s" % join.isouter
            print "join: %s" % join.join
            print "outerjoin: %s" % join.outerjoin
            print "params: %s" % join.params
            print "replace_selectable: %s" % join.replace_selectable
            print "select: %s" % join.select
            print "selectable: %s" % join.selectable
            print whoyadaddy
            print "==========================================="
            print "==========================================="
            print dir(join)
    
        # To Do: We need to handle RIGHT OUTER JOINS
        if join.isouter:
            join_type = " LEFT OUTER JOIN "
        else:
            join_type = " JOIN "
    
        # THis is looking for the as alias so we can interject them into the ON clause First on the LEFT and then on the RIGHT
        if left_raw.lower().find(" as ") > 0:
            t = left_raw.lower().split(" as ")
            left_table = t[-1]
        else:
            left_table = left_raw
        if right_raw.lower().find(" as ") > 0:
            t = right_raw.lower().split(" as ")
            right_table = t[-1]
        else:
            right_table = left_raw

        # This fixes the ON Clause and adds aliases
        o = onclause_raw.split(" = ")
        otmp = "%s.%s = %s.%s" % (left_table, o[0], right_table, o[1])
               
        return (
            join.left._compiler_dispatch(self, asfrom=True, **kwargs) +
            join_type +
            join.right._compiler_dispatch(self, asfrom=True, **kwargs) +
            " ON " + otmp   
        )

class DrillDDLCompiler(compiler.DDLCompiler):
    def get_column_specification(self, column, **kwargs):
        if column.table is None:
            raise exc.CompileError(
                "drill requires Table-bound columns "
                "in order to generate DDL")

        colspec = self.preparer.format_column(column)
        seq_col = column.table._autoincrement_column
        if seq_col is column:
            colspec += " AUTOINCREMENT"
        else:
            colspec += " " + self.dialect.type_compiler.process(column.type)

            if column.nullable is not None and not column.primary_key:
                if not column.nullable or column.primary_key:
                    colspec += " NOT NULL"
                else:
                    colspec += " NULL"

            default = self.get_column_default_string(column)
            if default is not None:
                colspec += " DEFAULT " + default

        return colspec


class DrillIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = compiler.RESERVED_WORDS.copy()
    reserved_words.update(['value', 'text', 'count', 'timestamp'])

    def __init__(self, dialect):
        super(DrillIdentifierPreparer, self). \
            __init__(dialect, initial_quote='`', final_quote='`')


class DrillDialect(default.DefaultDialect):

    colspecs = {}
    name = 'drill'
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False

    supports_simple_order_by_label = True

    poolclass = pool.SingletonThreadPool
    statement_compiler = DrillCompiler
    ddl_compiler = DrillDDLCompiler
    preparer = DrillIdentifierPreparer
    execution_ctx_cls = DrillExecutionContext

    @classmethod
    def dbapi(cls):
        import pyodbc as module
        return module

    def connect(self, *cargs, **cparams):
        return self.dbapi.connect(autocommit=True, *cargs, **cparams)

    def create_connect_args(self, url):
        opts = url.translate_connect_args()
        connectors = [""]
        if user:
            connectors.append("UID=%s" % user)
            connectors.append("PWD=%s" % opts.get("password", ""))
        return [[";".join(connectors)], {}]

    def has_table(self, connection, tablename, schema=None):
        result = connection.scalar(
            sql.text(
                "select * from name=:name limit 0"), name=tablename
        )
        return bool(result)

    def get_columns(self, connection, table_name, schema=None, **kw):
        q = "SELECT * FROM `%(table_id)s` LIMIT 0" % ({"table_id": table_name})
        columns = connection.execute(q)
        result = []
        for column_name in columns.keys():
            # TODO Handle types better            
            column = {
                "name": column_name,
                "type": VARCHAR,
                "default": None,
                "autoincrement": None,
                "nullable": False,
            }

            result.append(column)

        return result

    def get_table_names(self, connection, schema=None, **kw):
        result = connection.execute("show tables")
        table_names = [r[0] for r in result]
        return table_names

