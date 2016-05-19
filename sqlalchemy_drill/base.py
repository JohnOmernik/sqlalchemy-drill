# drill/base.py

"""
Support for the Apache Drill database.

"""
from sqlalchemy import processors
from sqlalchemy import sql, types, exc, pool, VARCHAR
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler







class AcNumeric(types.Numeric):
    def get_col_spec(self):
        return "NUMERIC"

    def bind_processor(self, dialect):
        return processors.to_str

    def result_processor(self, dialect, coltype):
        return None


class AcFloat(types.Float):
    def get_col_spec(self):
        return "FLOAT"

    def bind_processor(self, dialect):
        """By converting to string, we can use Decimal types round-trip."""
        return processors.to_str


class AcInteger(types.Integer):
    def get_col_spec(self):
        return "INTEGER"


class AcTinyInteger(types.Integer):
    def get_col_spec(self):
        return "TINYINT"


class AcSmallInteger(types.SmallInteger):
    def get_col_spec(self):
        return "SMALLINT"


class AcDateTime(types.DateTime):
    def get_col_spec(self):
        return "DATETIME"


class AcDate(types.Date):
    def get_col_spec(self):
        return "DATETIME"


class AcText(types.Text):
    def get_col_spec(self):
        return "MEMO"


class AcString(types.String):
    def get_col_spec(self):
        return "TEXT" + (self.length and ("(%d)" % self.length) or "")


class AcUnicode(types.Unicode):
    def get_col_spec(self):
        return "TEXT" + (self.length and ("(%d)" % self.length) or "")

    def bind_processor(self, dialect):
        return None

    def result_processor(self, dialect, coltype):
        return None


class AcChar(types.CHAR):
    def get_col_spec(self):
        return "TEXT" + (self.length and ("(%d)" % self.length) or "")


class AcBinary(types.LargeBinary):
    def get_col_spec(self):
        return "BINARY"


class AcBoolean(types.Boolean):
    def get_col_spec(self):
        return "YESNO"


class AcTimeStamp(types.TIMESTAMP):
    def get_col_spec(self):
        return "TIMESTAMP"


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

    ## Added as part of the original package (we may eventually remove)


    extract_map = compiler.SQLCompiler.extract_map.copy()
    extract_map.update({
        'month': 'm',
        'day': 'd',
        'year': 'yyyy',
        'second': 's',
        'hour': 'h',
        'doy': 'y',
        'minute': 'n',
        'quarter': 'q',
        'dow': 'w',
        'week': 'ww'
    })

    def visit_cast(self, cast, **kwargs):
        return cast.clause._compiler_dispatch(self, **kwargs)

    def visit_select_precolumns(self, select):
        """No pre column stuff with Drill"""
        return ""

    def limit_clause(self, select, **kwargs):
        """Limit in drill is after the select keyword"""
        text = ""
        if select._limit_clause is not None:
            text += "\n LIMIT " + self.process(select._limit_clause, **kwargs)
        return text

    def binary_operator_string(self, binary):
        """Drill uses "mod" instead of "%" """
        return binary.operator == '%' and 'mod' or binary.operator

    function_rewrites = {'current_date': 'now',
                         'current_timestamp': 'now',
                         'length': 'len',
                         }

    def visit_function(self, func, **kwargs):
        """Drill actually doesn't need this"""

        func.name = self.function_rewrites.get(func.name, func.name)
        return super(DrillCompiler, self).visit_function(func)

    def for_update_clause(self, select):
        """FOR UPDATE is not supported by Drill; silently ignore"""
        return ''

    # Strip schema
    def visit_table(self, table, asfrom=False, **kwargs):
        if asfrom:
            return self.preparer.quote(table.name, '`')
        else:
            return ""

    def visit_join(self, join, asfrom=False, **kwargs):
        
        return ( self.process(join.left, asfrom=True) + \
                (join.isouter and " LEFT OUTER JOIN " or " INNER JOIN ") + \
                self.process(join.right, asfrom=True) + " ON " + \
                self.process(join.onclause) )

    def visit_extract(self, extract, **kw):
        field = self.extract_map.get(extract.field, extract.field)
        return 'DATEPART("%s", %s)' % \
               (field, self.process(extract.expr, **kw))


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
    reserved_words.update(['value', 'text'])

    def __init__(self, dialect):
        super(DrillIdentifierPreparer, self). \
            __init__(dialect, initial_quote='`', final_quote='`')


class DrillDialect(default.DefaultDialect):
    colspecs = {
        types.Unicode: AcUnicode,
        types.Integer: AcInteger,
        types.SmallInteger: AcSmallInteger,
        types.Numeric: AcNumeric,
        types.Float: AcFloat,
        types.DateTime: AcDateTime,
        types.Date: AcDate,
        types.String: AcString,
        types.LargeBinary: AcBinary,
        types.Boolean: AcBoolean,
        types.Text: AcText,
        types.CHAR: AcChar,
        types.TIMESTAMP: AcTimeStamp,
    }
    name = 'drill'
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False

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
        connectors.append("Dbq=%s" % opts["database"])
        user = opts.get("username", None)
        if user:
            connectors.append("UID=%s" % user)
            connectors.append("PWD=%s" % opts.get("password", ""))
        return [[";".join(connectors)], {}]

    def last_inserted_ids(self):
        """
        Drill doesn't insert.
        """
        return []

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

    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        """
        Drill doesn't have primary keys, return []
        """
        return []

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """
        Drill does not have foreign keys, return []
        """

        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        """
        Drill doesn't have indexes, return []
        """

        return []
